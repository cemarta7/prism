<?php

declare(strict_types=1);

namespace Prism\Prism\Providers\Replicate\Handlers;

use Generator;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Str;
use Prism\Prism\Exceptions\PrismException;
use Prism\Prism\Exceptions\PrismStreamDecodeException;
use Prism\Prism\Providers\Replicate\Concerns\HandlesPredictions;
use Prism\Prism\Providers\Replicate\Maps\FinishReasonMap;
use Prism\Prism\Providers\Replicate\Maps\MessageMap;
use Prism\Prism\Streaming\EventID;
use Prism\Prism\Streaming\Events\StreamEndEvent;
use Prism\Prism\Streaming\Events\StreamEvent;
use Prism\Prism\Streaming\Events\StreamStartEvent;
use Prism\Prism\Streaming\Events\TextCompleteEvent;
use Prism\Prism\Streaming\Events\TextDeltaEvent;
use Prism\Prism\Streaming\Events\TextStartEvent;
use Prism\Prism\Streaming\StreamState;
use Prism\Prism\Text\Request;
use Prism\Prism\ValueObjects\Usage;
use Psr\Http\Message\StreamInterface;
use Throwable;

class Stream
{
    use HandlesPredictions;

    protected StreamState $state;

    public function __construct(
        protected PendingRequest $client,
        protected int $pollingInterval,
        protected int $maxWaitTime
    ) {
        $this->state = new StreamState;
    }

    /**
     * @return Generator<StreamEvent>
     */
    public function handle(Request $request): Generator
    {
        $this->state->reset()->withMessageId(EventID::generate());

        // Build the prompt from messages
        $prompt = MessageMap::map($request->messages());

        // Prepare the prediction payload with stream enabled
        $payload = [
            'version' => $this->extractVersionFromModel($request->model()),
            'input' => array_merge(
                ['prompt' => $prompt],
                $this->buildInputParameters($request)
            ),
            'stream' => true, // Enable streaming
        ];

        // Create prediction
        $prediction = $this->createPrediction($this->client, $payload);

        // Emit stream start
        yield new StreamStartEvent(
            id: EventID::generate(),
            timestamp: time(),
            model: $request->model(),
            provider: 'replicate',
        );

        // Check if streaming URL is available
        $streamUrl = $prediction->urls['stream'] ?? null;

        if ($streamUrl !== null) {
            // Use real-time SSE streaming
            yield from $this->processSSEStream($streamUrl, $prediction->id);
        } else {
            // Fallback to simulated streaming (poll + tokenize)
            $completedPrediction = $this->waitForPrediction(
                $this->client,
                $prediction->id,
                $this->pollingInterval,
                $this->maxWaitTime
            );

            yield from $this->processTokenizedOutput($completedPrediction);
        }
    }

    /**
     * Process real-time SSE stream from Replicate.
     *
     * @return Generator<StreamEvent>
     */
    protected function processSSEStream(string $streamUrl, string $predictionId): Generator
    {
        // Connect to the SSE stream
        $response = $this->client->withOptions(['stream' => true])->get($streamUrl);
        $stream = $response->getBody();

        $textStarted = false;
        $finalStatus = 'succeeded';
        $metrics = [];

        try {
            while (! $stream->eof()) {
                $data = $this->parseNextDataLine($stream);

                if ($data === null) {
                    continue;
                }

                // Handle different SSE event types
                $event = $data['event'] ?? null;

                if ($event === 'output') {
                    // Text output event
                    if (! $textStarted) {
                        yield new TextStartEvent(
                            id: EventID::generate(),
                            timestamp: time(),
                            messageId: $this->state->messageId()
                        );
                        $textStarted = true;
                    }

                    $token = $data['data'] ?? '';

                    if ($token !== '') {
                        $this->state->appendText($token);

                        yield new TextDeltaEvent(
                            id: EventID::generate(),
                            timestamp: time(),
                            delta: $token,
                            messageId: $this->state->messageId()
                        );
                    }
                } elseif ($event === 'done') {
                    // Stream completion event
                    $finalStatus = $data['status'] ?? 'succeeded';
                    $metrics = $data['metrics'] ?? [];
                    break;
                } elseif ($event === 'error') {
                    // Error event
                    $errorMessage = $data['error'] ?? 'Unknown streaming error';
                    throw new PrismException("Replicate streaming error: {$errorMessage}");
                }
            }
        } finally {
            $stream->close();
        }

        // Emit text complete if text was started
        if ($textStarted) {
            yield new TextCompleteEvent(
                id: EventID::generate(),
                timestamp: time(),
                messageId: $this->state->messageId()
            );
        }

        // Emit stream end
        yield new StreamEndEvent(
            id: EventID::generate(),
            timestamp: time(),
            finishReason: FinishReasonMap::map($finalStatus),
            usage: new Usage(
                promptTokens: $metrics['input_token_count'] ?? 0,
                completionTokens: $metrics['output_token_count'] ?? 0,
            ),
        );
    }

    /**
     * Parse next SSE data line from stream.
     *
     * @return array<string, mixed>|null
     */
    protected function parseNextDataLine(StreamInterface $stream): ?array
    {
        $line = $this->readLine($stream);

        if (! str_starts_with($line, 'data:')) {
            return null;
        }

        $line = trim(substr($line, strlen('data: ')));

        // Check for stream end markers
        if ($line === '' || Str::contains($line, 'done')) {
            // Try to parse as JSON in case it's a done event with data
            if (Str::startsWith($line, '{')) {
                try {
                    return json_decode($line, true, flags: JSON_THROW_ON_ERROR);
                } catch (Throwable) {
                    return null;
                }
            }

            return null;
        }

        try {
            return json_decode($line, true, flags: JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            throw new PrismStreamDecodeException('Replicate', $e);
        }
    }

    /**
     * Read a single line from the stream.
     */
    protected function readLine(StreamInterface $stream): string
    {
        $buffer = '';

        while (! $stream->eof()) {
            $byte = $stream->read(1);

            if ($byte === '') {
                return $buffer;
            }

            $buffer .= $byte;

            if ($byte === "\n") {
                break;
            }
        }

        return $buffer;
    }

    /**
     * Process tokenized output as streaming events (fallback method).
     *
     * @return Generator<StreamEvent>
     */
    protected function processTokenizedOutput($prediction): Generator
    {
        $output = $prediction->output ?? [];

        if (! is_array($output)) {
            $output = [$output];
        }

        // Emit text start
        yield new TextStartEvent(
            id: EventID::generate(),
            timestamp: time(),
            messageId: $this->state->messageId()
        );

        // Stream each token as a delta
        foreach ($output as $token) {
            if (is_string($token) && $token !== '') {
                $this->state->appendText($token);

                yield new TextDeltaEvent(
                    id: EventID::generate(),
                    timestamp: time(),
                    delta: $token,
                    messageId: $this->state->messageId()
                );
            }
        }

        // Emit text complete
        yield new TextCompleteEvent(
            id: EventID::generate(),
            timestamp: time(),
            messageId: $this->state->messageId()
        );

        // Emit stream end
        yield new StreamEndEvent(
            id: EventID::generate(),
            timestamp: time(),
            finishReason: FinishReasonMap::map($prediction->status),
            usage: new Usage(
                promptTokens: $prediction->metrics['input_token_count'] ?? 0,
                completionTokens: $prediction->metrics['output_token_count'] ?? 0,
            ),
        );
    }

    /**
     * Build input parameters from request.
     *
     * @return array<string, mixed>
     */
    protected function buildInputParameters(Request $request): array
    {
        $params = [];

        if ($request->maxTokens()) {
            $params['max_tokens'] = $request->maxTokens();
        }

        // Map provider options
        foreach ($request->providerOptions() as $key => $value) {
            $params[$key] = $value;
        }

        return $params;
    }

    /**
     * Extract version from model string.
     */
    protected function extractVersionFromModel(string $model): string
    {
        // If model already contains a version hash, use it
        if (str_contains($model, ':')) {
            [, $version] = explode(':', $model, 2);

            return $version;
        }

        // Use default model version
        return match ($model) {
            'meta/meta-llama-3.1-405b-instruct' => 'hidden', // Replicate will resolve this
            default => $model,
        };
    }
}

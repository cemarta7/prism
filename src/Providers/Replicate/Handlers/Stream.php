<?php

declare(strict_types=1);

namespace Prism\Prism\Providers\Replicate\Handlers;

use Generator;
use Illuminate\Http\Client\PendingRequest;
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

        // Prepare the prediction payload
        $payload = [
            'version' => $this->extractVersionFromModel($request->model()),
            'input' => array_merge(
                ['prompt' => $prompt],
                $this->buildInputParameters($request)
            ),
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

        // Wait for completion
        $completedPrediction = $this->waitForPrediction(
            $this->client,
            $prediction->id,
            $this->pollingInterval,
            $this->maxWaitTime
        );

        // Process the output as a stream
        yield from $this->processTokenizedOutput($completedPrediction, $request);
    }

    /**
     * Process tokenized output as streaming events.
     *
     * @return Generator<StreamEvent>
     */
    protected function processTokenizedOutput($prediction, Request $request): Generator
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

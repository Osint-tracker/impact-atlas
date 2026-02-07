
Batch API
Process jobs asynchronously with Batch API.
Learn how to use OpenAI's Batch API to send asynchronous groups of requests with 50% lower costs, a separate pool of significantly higher rate limits, and a clear 24-hour turnaround time. The service is ideal for processing jobs that don't require immediate responses. You can also explore the API reference directly here.

Overview
While some uses of the OpenAI Platform require you to send synchronous requests, there are many cases where requests do not need an immediate response or rate limits prevent you from executing a large number of queries quickly. Batch processing jobs are often helpful in use cases like:

Running evaluations
Classifying large datasets
Embedding content repositories
The Batch API offers a straightforward set of endpoints that allow you to collect a set of requests into a single file, kick off a batch processing job to execute these requests, query for the status of that batch while the underlying requests execute, and eventually retrieve the collected results when the batch is complete.

Compared to using standard endpoints directly, Batch API has:

Better cost efficiency: 50% cost discount compared to synchronous APIs
Higher rate limits: Substantially more headroom compared to the synchronous APIs
Fast completion times: Each batch completes within 24 hours (and often more quickly)
Getting started
1. Prepare your batch file
Batches start with a .jsonl file where each line contains the details of an individual request to the API. For now, the available endpoints are /v1/responses (Responses API), /v1/chat/completions (Chat Completions API), /v1/embeddings (Embeddings API), /v1/completions (Completions API), and /v1/moderations (Moderations guide). For a given input file, the parameters in each line's body field are the same as the parameters for the underlying endpoint. Each request must include a unique custom_id value, which you can use to reference results after completion. Here's an example of an input file with 2 requests. Note that each input file can only include requests to a single model.

When targeting /v1/moderations, include an input field in every request body. Batch accepts both plain-text inputs (for omni-moderation-latest and text-moderation-latest) and multimodal content arrays (for omni-moderation-latest). The Batch worker enforces the same non-streaming requirement as the synchronous Moderations API and rejects requests that set stream=true.

{"custom_id": "request-1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gpt-3.5-turbo-0125", "messages": [{"role": "system", "content": "You are a helpful assistant."},{"role": "user", "content": "Hello world!"}],"max_tokens": 1000}}
{"custom_id": "request-2", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "gpt-3.5-turbo-0125", "messages": [{"role": "system", "content": "You are an unhelpful assistant."},{"role": "user", "content": "Hello world!"}],"max_tokens": 1000}}
Moderations input examples
Text-only request:

{"custom_id": "moderation-text-1", "method": "POST", "url": "/v1/moderations", "body": {"model": "omni-moderation-latest", "input": "This is a harmless test sentence."}}
Multimodal request:

{"custom_id": "moderation-mm-1", "method": "POST", "url": "/v1/moderations", "body": {"model": "omni-moderation-latest", "input": [{"type": "text", "text": "Describe this image"}, {"type": "image_url", "image_url": {"url": "https://upload.wikimedia.org/wikipedia/commons/3/3f/Fronalpstock_big.jpg"}}]}}
Prefer referencing remote assets with image_url (instead of base64 blobs) to keep your .jsonl files well below the 200 MB Batch upload limit, especially for multimodal Moderations requests.

2. Upload your batch input file
Similar to our Fine-tuning API, you must first upload your input file so that you can reference it correctly when kicking off batches. Upload your .jsonl file using the Files API.

Upload files for Batch API
import fs from "fs";
import OpenAI from "openai";
const openai = new OpenAI();

const file = await openai.files.create({
  file: fs.createReadStream("batchinput.jsonl"),
  purpose: "batch",
});

console.log(file);
3. Create the batch
Once you've successfully uploaded your input file, you can use the input File object's ID to create a batch. In this case, let's assume the file ID is file-abc123. For now, the completion window can only be set to 24h. You can also provide custom metadata via an optional metadata parameter.

Create the Batch
import OpenAI from "openai";
const openai = new OpenAI();

const batch = await openai.batches.create({
  input_file_id: "file-abc123",
  endpoint: "/v1/chat/completions",
  completion_window: "24h"
});

console.log(batch);
This request will return a Batch object with metadata about your batch:

{
  "id": "batch_abc123",
  "object": "batch",
  "endpoint": "/v1/chat/completions",
  "errors": null,
  "input_file_id": "file-abc123",
  "completion_window": "24h",
  "status": "validating",
  "output_file_id": null,
  "error_file_id": null,
  "created_at": 1714508499,
  "in_progress_at": null,
  "expires_at": 1714536634,
  "completed_at": null,
  "failed_at": null,
  "expired_at": null,
  "request_counts": {
    "total": 0,
    "completed": 0,
    "failed": 0
  },
  "metadata": null
}
4. Check the status of a batch
You can check the status of a batch at any time, which will also return a Batch object.

Check the status of a batch
import OpenAI from "openai";
const openai = new OpenAI();

const batch = await openai.batches.retrieve("batch_abc123");
console.log(batch);
The status of a given Batch object can be any of the following:

Status	Description
validating	the input file is being validated before the batch can begin
failed	the input file has failed the validation process
in_progress	the input file was successfully validated and the batch is currently being run
finalizing	the batch has completed and the results are being prepared
completed	the batch has been completed and the results are ready
expired	the batch was not able to be completed within the 24-hour time window
cancelling	the batch is being cancelled (may take up to 10 minutes)
cancelled	the batch was cancelled
5. Retrieve the results
Once the batch is complete, you can download the output by making a request against the Files API via the output_file_id field from the Batch object and writing it to a file on your machine, in this case batch_output.jsonl

Retrieving the batch results
import OpenAI from "openai";
const openai = new OpenAI();

const fileResponse = await openai.files.content("file-xyz123");
const fileContents = await fileResponse.text();

console.log(fileContents);
The output .jsonl file will have one response line for every successful request line in the input file. Any failed requests in the batch will have their error information written to an error file that can be found via the batch's error_file_id.

Note that the output line order may not match the input line order. Instead of relying on order to process your results, use the custom_id field which will be present in each line of your output file and allow you to map requests in your input to results in your output.

{"id": "batch_req_123", "custom_id": "request-2", "response": {"status_code": 200, "request_id": "req_123", "body": {"id": "chatcmpl-123", "object": "chat.completion", "created": 1711652795, "model": "gpt-3.5-turbo-0125", "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello."}, "logprobs": null, "finish_reason": "stop"}], "usage": {"prompt_tokens": 22, "completion_tokens": 2, "total_tokens": 24}, "system_fingerprint": "fp_123"}}, "error": null}
{"id": "batch_req_456", "custom_id": "request-1", "response": {"status_code": 200, "request_id": "req_789", "body": {"id": "chatcmpl-abc", "object": "chat.completion", "created": 1711652789, "model": "gpt-3.5-turbo-0125", "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hello! How can I assist you today?"}, "logprobs": null, "finish_reason": "stop"}], "usage": {"prompt_tokens": 20, "completion_tokens": 9, "total_tokens": 29}, "system_fingerprint": "fp_3ba"}}, "error": null}
The output file will automatically be deleted 30 days after the batch is complete.

6. Cancel a batch
If necessary, you can cancel an ongoing batch. The batch's status will change to cancelling until in-flight requests are complete (up to 10 minutes), after which the status will change to cancelled.

Cancelling a batch
import OpenAI from "openai";
const openai = new OpenAI();

const batch = await openai.batches.cancel("batch_abc123");
console.log(batch);
7. Get a list of all batches
At any time, you can see all your batches. For users with many batches, you can use the limit and after parameters to paginate your results.

Getting a list of all batches
import OpenAI from "openai";
const openai = new OpenAI();

const list = await openai.batches.list();


for await (const batch of list) {
  console.log(batch);
}
Model availability
The Batch API is widely available across most of our models, but not all. Please refer to the model reference docs to ensure the model you're using supports the Batch API.

Rate limits
Batch API rate limits are separate from existing per-model rate limits. The Batch API has three types of rate limits:

Per-batch limits: A single batch may include up to 50,000 requests, and a batch input file can be up to 200 MB in size. Note that /v1/embeddings batches are also restricted to a maximum of 50,000 embedding inputs across all requests in the batch.
Enqueued prompt tokens per model: Each model has a maximum number of enqueued prompt tokens allowed for batch processing. You can find these limits on the Platform Settings page.
Batch creation rate limit: You can create up to 2,000 batches per hour. If you need to submit more requests, increase the number of requests per batch.
There are no limits for output tokens for the Batch API today. Because Batch API rate limits are a new, separate pool, using the Batch API will not consume tokens from your standard per-model rate limits, thereby offering you a convenient way to increase the number of requests and processed tokens you can use when querying our API.

Batch expiration
Batches that do not complete in time eventually move to an expired state; unfinished requests within that batch are cancelled, and any responses to completed requests are made available via the batch's output file. You will be charged for tokens consumed from any completed requests.

Expired requests will be written to your error file with the message as shown below. You can use the custom_id to retrieve the request data for expired requests.

{"id": "batch_req_123", "custom_id": "request-3", "response": null, "error": {"code": "batch_expired", "message": "This request could not be executed before the completion window expired."}}
{"id": "batch_req_123", "custom_id": "request-7", "response": null, "error": {"code": "batch_expired", "message": "This request could not be executed before the completion window expired."}}


Docs
API reference
Embeddings
Get a vector representation of a given input that can be easily consumed by machine learning models and algorithms. Related guide: Embeddings

Create embeddings
post
 
https://api.openai.com/v1/embeddings
Creates an embedding vector representing the input text.

Request body
input
string or array

Required
Input text to embed, encoded as a string or array of tokens. To embed multiple inputs in a single request, pass an array of strings or array of token arrays. The input must not exceed the max input tokens for the model (8192 tokens for all embedding models), cannot be an empty string, and any array must be 2048 dimensions or less. Example Python code for counting tokens. In addition to the per-input token limit, all embedding models enforce a maximum of 300,000 tokens summed across all inputs in a single request.

model
string

Required
ID of the model to use. You can use the List models API to see all of your available models, or see our Model overview for descriptions of them.

dimensions
integer

Optional
The number of dimensions the resulting output embeddings should have. Only supported in text-embedding-3 and later models.

encoding_format
string

Optional
Defaults to float
The format to return the embeddings in. Can be either float or 
base64
.

user
string

Optional
A unique identifier representing your end-user, which can help OpenAI to monitor and detect abuse. Learn more.

Returns
A list of embedding objects.

Example request
curl https://api.openai.com/v1/embeddings \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "input": "The food was delicious and the waiter...",
    "model": "text-embedding-ada-002",
    "encoding_format": "float"
  }'
Response
{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [
        0.0023064255,
        -0.009327292,
        .... (1536 floats total for ada-002)
        -0.0028842222,
      ],
      "index": 0
    }
  ],
  "model": "text-embedding-ada-002",
  "usage": {
    "prompt_tokens": 8,
    "total_tokens": 8
  }
}
The embedding object
Represents an embedding vector returned by embedding endpoint.

embedding
array

The embedding vector, which is a list of floats. The length of vector depends on the model as listed in the embedding guide.

index
integer

The index of the embedding in the list of embeddings.

object
string

The object type, which is always "embedding".

OBJECT The embedding object
{
  "object": "embedding",
  "embedding": [
    0.0023064255,
    -0.009327292,
    .... (1536 floats total for ada-002)
    -0.0028842222,
  ],
  "index": 0
}

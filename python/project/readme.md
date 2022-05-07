For production, we should:

1. store Astra token in a secret store (instead of pulling from environment variable) so we can more easily lifecycle the token
2. separate functions into separate projects with their own requirements.txt files so we can reduce the package size to improve scalability and cold start time
3. make tests more robust to refactoring by using more dependency injection to create seams and by using more mocks, stubs, fakes to isolate code under test.
4. Evaluate race conditions in Astra DB when writing with parallelism > 1
5. Use async Pulsar producer
6. Handle intermittent failures to produce
7. Refactor producers to common layer to reduce code duplication
8. Decide how to better handle incomplete records and introduce OOP

Homework for Contoso corp:
1. Add missing fields to RSS feed
2. Create new feed with resolutions so we can update those in real time
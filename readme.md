For production, we should:

1. store Astra token in a secret store (instead of pulling from environment variable) so we can more easily lifecycle the token
2. separate functions into separate projects with their own requirements.txt files so we can reduce the package size to improve scalability and cold start time
3. 

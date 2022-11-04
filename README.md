# AWS S3 Deployment Cleanup

A simple python3 script to clean up old deployments of content to AWS S3. The script will connect
to your specified AWS profile (default is `default`), scan the requested bucket for all common prefixes, 
order them by deployment date in descending order, and delete all prefixes greater than the desired 
retention `count`. Optionally, a `days` parameter can be provided that will delete all prefixes 
greater than `N` days as long as the minimum `count` is also met. _(i.e. if the desired `count` is 
`5` and `days` is `30` but only 4 deployments have happened in the last 30 days, the next most recent 
deployment beyond 30 days will also be retained.)_

```
Usage: s3dc.py [OPTIONS] BUCKET_NAME COUNT

Options:
  -d, --days INTEGER     Maximum days to retain deployments. (Must also meet
                         minimum COUNT requirements.)
  -e, --endpoint TEXT    Endpoint URL
  -p, --profile TEXT     AWS credentials profile to use for this session.
  -r, --region TEXT      Region for S3 bucket connection.
  -t, --timeout INTEGER  Default connect/read timeouts for S3 connection.
  --help                 Show this message and exit.
```

### Assumptions
* AWS S3 stores each object with the _path_ as the key but there are no such things as `directories` 
  like a typical block storage device. The timestamp of the first object with a given prefix will be used 
  as the timestamp for the deployment. This timestamp may not be representative of the earliest timestamp 
  in the prefix but has been used as a "close enough" representation as an optimization to avoid querying 
  every object in the bucket. If deployments  

### TODO:
* Add key/prefix count tracking for output visibility
* Add tests
* Add Github action to execute test suite
  * Spin up localstack (https://docs.localstack.cloud/ci/github-actions/) or mock responses

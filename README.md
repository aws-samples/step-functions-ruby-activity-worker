## Step Functions Ruby Activity Worker

The code contains a example implementation of Step Functions activity worker written in Ruby.

## Summary

This package defines the Step Functions reference activity worker implementation.

The code implements consumer-producer pattern with the configured number of
threads for pollers and activity workers. The pollers threads are constantly
long polling the activity task. Once a a activity task is retrieved it is being
passing through bound blocking queue for the activity thread to pick it up.

## Setting up the code

### Install Ruby AWS SDK

```bash
gem install aws-sdk
```

Install AWS CLI and run:

```bash
aws configure
```

Setup your access key and secret key to your AWS IAM user.

Open `bin/run_activity.rb` script and configure the ACTIVITY_ARN and region.

### Run:

```bash
bin/run_activity.rb
```

## License

This library is licensed under the Apache 2.0 License. 

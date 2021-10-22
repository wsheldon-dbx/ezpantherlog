# ezpantherlog

Automatically create runpanther.io schemas based on a sample of JSON logs.

## Usage:
```
Usage: ezpantherlog.py [OPTIONS]

Options:
  --logs PATH              The sample of logs you are building a schema for;
                           evaluated relative to the current path, not the git
                           root.  [required]
  --pantherlog-dir PATH    The full path to your pantherlog binary; evaluated
                           relative to the current path, not the git root.
                           [required]
  --schema-name TEXT       The name of the schema to use for the `schema` key
                           in your schema file; if you don't add `Custom.` we
                           add it for you.  [required]
  --schema-file-name TEXT  The name to give your schema file; example: ldap
                           [required]
  --event-time-field TEXT  The field that will be used as your isEventTime
                           key.  [required]
  --help                   Show this message and exit.
```


## Example:

```
python3 ezpantherlog.py --logs=/tmp/corpnet.jsonl \
                        --schema-file-name=vpn \
                        --schema-name=Custom.VPN \
                        --event-time-field=syslogTimestamp \
                        --pantherlog-dir=/Users/wsheldon/Tools/pantherlog1.21

✨ Starting...
💥 Inferring your schema
🔥 Parsing your logs
💫 Testing your schema
🌟 All tests passed!

   -> /Users/wsheldon/Tools/ezpantherlog/vpn.yml
   -> /Users/wsheldon/Tools/ezpantherlog/vpn_tests.yml

🚨 Remember to update your schema file with indicators.

   Reference:
    - https://docs.runpanther.io/development/writing-parsers#indicator-strings
    - https://docs.runpanther.io/data-onboarding/custom-log-types/reference#indicators
```
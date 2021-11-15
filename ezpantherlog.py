import os
import sys
import time
import json
import subprocess
import ruamel.yaml
from typing import Optional, List, Union, Any

import click
import ujson


class LogFormatError(Exception):
    pass

class SchemaNameError(Exception):
    pass

class EventTimeFieldError(Exception):
    pass

class TimeFormatError(Exception):
    pass

class PantherlogParseError(Exception):
    pass

class PantherlogTestError(Exception):
    pass


def run(cmd: str, return_list: bool = False) -> Union[List[str], Any]:
    return subprocess.run(cmd, capture_output=True, cwd=None, shell=True)


def _validate_logs(ctx: Optional, param: Optional, value: str) -> str:
    with open(value, "r") as file:
        first_line = file.readline()
        for last_line in file:
            pass

    first_line = first_line.strip()
    last_line = last_line.strip()

    if not first_line.endswith("}"):
        raise LogFormatError(
            "The JSON log sample must be one blob per log line (must not be pretty printed)."
        )
    if not last_line.startswith("{"):
        raise LogFormatError(
            "The JSON log sample must be one blob per log line (must not be pretty printed)."
        )

    with open(value, "r") as file:
        for num, line in enumerate(file, start=1):
            try:
                ujson.loads(line)
            except ValueError:
                raise LogFormatError(
                    f"The JSON sample on line {num} failed to be loaded, is the JSON valid?"
                )

    return value


def _validate_schema_name(ctx: Optional, param: Optional, value: str) -> str:
    if value.startswith("Custom.") is False:
        value = "Custom." + value

    if value.partition(".")[2][0].isupper() is False:
        raise SchemaNameError(f"{value} must start with a capital letter.")

    return value


def _validate_time_format(ctx: Optional, param: Optional, value: str) -> str:

    BUILT_IN_FORMATS = [
        'rfc3339',
        'unix',
        'unix_ms',
        'unix_us',
        'unix_ns',
    ]

    if value not in BUILT_IN_FORMATS and value is not None:
        raise TimeFormatError(f"{value} is not a valid time format, must be one of {BUILT_IN_FORMATS}")

    return value


def _is_event_time_field_missing(schema: dict, event_time_field: str) -> bool:
    for value in schema["fields"]:
        if value["name"] == event_time_field:
            return True
    return False


@click.command()
@click.option(
    "--logs",
    required=True,
    prompt=True,
    type=click.Path(exists=True, file_okay=True),
    callback=_validate_logs,
    help="The sample of logs you are building a schema for; evaluated relative to the current path, not the git root.",
)
@click.option(
    "--pantherlog-dir",
    required=True,
    prompt=True,
    type=click.Path(exists=True, file_okay=True),
    help="The full path to your pantherlog binary; evaluated relative to the current path, not the git root.",
)
@click.option(
    "--schema-name",
    required=True,
    prompt=True,
    callback=_validate_schema_name,
    type=str,
    help="The name of the schema to use for the `schema` key in your schema file; if you don't add `Custom.` we add it for you.",
)
@click.option(
    "--schema-file-name",
    required=True,
    prompt=True,
    type=str,
    help="The name to give your schema file; example: ldap",
)
@click.option(
    "--event-time-field",
    required=True,
    prompt=True,
    type=str,
    help="The field that will be used as your isEventTime key.",
)
@click.option(
    "--time-format",
    required=False,
    prompt=False,
    callback=_validate_time_format,
    type=str,
    help="Timestamps are defined by setting the type field to timestamp and specifying the timestamp format using the timeFormat field.",
)
@click.option(
    "--json-field",
    required=False,
    prompt=False,
    type=str,
    multiple=True,
    help="Any valid JSON value (JSON object, array, number, string, boolean)",
)
def main(
    logs: str,
    pantherlog_dir: str,
    schema_name: str,
    schema_file_name: str,
    event_time_field: str,
    time_format: str,
    json_field: str,
) -> None:

    print("\n✨ Starting...")
    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)

    cwd = os.getcwd()
    schema_file = schema_file_name + ".yml"
    schema_test_file = schema_file_name + "_tests.yml"

    CMDS = {
        "infer": f"{pantherlog_dir} infer {logs} > {schema_file}",
        "parse": f"{pantherlog_dir} parse --path {cwd}/{schema_file} --schemas {schema_name} {logs}",
        "test": f"{pantherlog_dir} test {cwd}/{schema_file} {schema_test_file}",
    }

    print(f"💥 Inferring your schema")
    run(CMDS["infer"])

    while not os.path.exists(schema_file):
        time.sleep(0.1)

    with open(schema_file, "r") as original:
        data = original.read()

    with open(schema_file, "w") as modified:
        modified.write(f"schema: {schema_name}\n" + data)

    with open(schema_file, "r") as yamlfile:
        schema = yaml.load(yamlfile)

        if _is_event_time_field_missing(schema, event_time_field) is False:
            raise EventTimeFieldError(
                f"Unable to find eventTimeField {event_time_field}"
            )

        for field in schema["fields"]:
            if field["name"] == event_time_field:
                field["isEventTime"] = True
                field["type"] = "timestamp"

                if time_format:
                    field["timeFormat"] = time_format

            if json_field:
                for jfield in json_field:
                    if field["name"] == jfield:
                        field["type"] = "json"
                        del field["fields"]

    if schema:
        with open(schema_file, "w") as yamlfile:
            yaml.dump(schema, yamlfile)

    print(f"🔥 Parsing your logs")
    parsed = run(CMDS["parse"])
    
    if 'parse error' in parsed.stderr.decode("utf-8"):
        err = parsed.stderr.decode("utf-8")
        raise PantherlogParseError(f"pantherlog failed to parse. Exception from pantherlog: \n\n {err}")

    lines = parsed.stdout.splitlines()
    last_parsed_line = lines[-1]

    json_object = json.loads(last_parsed_line)
    json_formatted_str = json.dumps(json_object, indent=4)

    with open(f"{logs}", "r") as f:
        last_line = f.readlines()[-1]

    sample_event_json = json.loads(last_line)
    sample_event_json_formatted = json.dumps(sample_event_json, indent=4)

    with open(schema_test_file, "w") as test_file:
        test_file.write(f"# Generated by ezpantherlog: https://github.com/wsheldon-dbx/ezpantherlog\n")
        test_file.write(f"logType: {schema_name}\n")
        test_file.write(f"input: |\n")
        test_file.write(f"result: |\n")

    with open(schema_test_file, "r") as test_yaml_file:
        cur_yaml_test = yaml.load(test_yaml_file)

    with open(schema_test_file, "w") as test_file_yaml:
        cur_yaml_test["input"] = sample_event_json_formatted
        cur_yaml_test["result"] = json_formatted_str
        yaml.dump(cur_yaml_test, test_file_yaml)

    print(f"💫 Testing your schema")
    proc = run(CMDS["test"])
    if "PASS" in str(proc.stderr):
        print(f"🌟 All tests passed!")
    else:
        err = proc.stderr.decode("utf-8")
        raise PantherlogTestError(f"pantherlog test failed. Exception from pantherlog: \n\n {err}")

    print(f"\n   -> {cwd}/{schema_file}")
    print(f"   -> {cwd}/{schema_test_file}")

    print("\n🚨 Remember to update your schema file with indicators.")

    print("\n   Reference:")
    print(
        "    - https://docs.runpanther.io/development/writing-parsers#indicator-strings"
    )
    print(
        "    - https://docs.runpanther.io/data-onboarding/custom-log-types/reference#indicators"
    )


if __name__ == "__main__":
    main()

"""
qhist module for querying PBS Pro historical job records

Todo:
    * Derived fields specified on the command-line or in config
    * More statistics
    * Client-server mode of operation
    * Memory-friendly sorting
"""

import sys, os, argparse, datetime, signal, string, _string, json, operator, re, importlib, textwrap

from collections import OrderedDict
from json.decoder import JSONDecodeError
from pbsparse import get_pbs_records
from glob import glob


# Use default signal behavior on system rather than throwing IOError
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Constants
ONE_DAY = datetime.timedelta(days = 1)
EMPTY_DATETIME = datetime.datetime(1,1,1)

# Long-form help statements
qhist_help = """
This command allows you to query the PBS accounting records for finished jobs.
Any job that has an end-type record (E,R) can be queried. If a job has not yet
completed, it will not appear here and users should consult "qstat" instead. By
default, qhist will only show jobs from the current calendar day, but this can
be customized. Many options are available for filtering data. Most of them
support negation by prepending the search value by the "~" character (such
values should be encapsulated in quotation marks to avoid shell substitutions).
"""

format_help = """
This option allows you to specify a custom format. This setting's behavior
depends on which mode you are using:

For default and wide behavior, enter a string containing Python's format syntax
(modern version). For list and csv modes, a comma-delimited string with field
names is the expected input.

Examples:
    qhist --format="{id:9.9} {account:9.9} {reqmem:8.2f} {memory:8.2f}"
    qhist --list --format="account,reqmem,memory"

The following variables are available:
"""

filter_help = """
This option allows you to filter job data by a semicolon-delimited list of fields
and expressions. Note that '<', '>', and '~' (not) will be interpreted by the
shell and thus you should encapsulate your expression in quotes.

Examples:
    qhist --filter="cputype==milan;ompthreads>1"
    qhist --filter="~queue==cpu"

The following fields are available:
"""

#
## Classes
#

class FillFormatter(string.Formatter):
    def __init__(self, fill_value = ""):
        self.fill_value = fill_value

    def get_value(self, key, args, kwargs):
        if isinstance(key, int):
            return args[key]
        else:
            return kwargs.get(key, "fill_value")

    def get_field(self, field_name, args, kwargs):
        first, rest = _string.formatter_field_name_split(field_name)

        obj = self.get_value(first, args, kwargs)

        # loop through the rest of the field_name, doing
        #  getattr or getitem as needed

        if obj != "fill_value":
            for is_attr, i in rest:
                if is_attr:
                    obj = getattr(obj, i)
                else:
                    try:
                        obj = obj[i]
                    except KeyError:
                        obj = "fill_value"

        return obj, first

    def format_field(self, value, format_spec):
        if value != "fill_value":
            try:
                return format(value, format_spec)
            except:
                print(value, format_spec)
                sys.exit()
        elif "%" not in format_spec:
            return format(self.fill_value, format_spec.translate(str.maketrans("", "", "fd+= ")))
        else:
            return " " * (len(format(EMPTY_DATETIME, format_spec)) - len(self.fill_value)) + self.fill_value

class QhistConfig:
    def __init__(self, default_config = None, time_format = "h"):
        if not default_config:
            default_config = os.path.join(os.path.dirname(__file__), 'cfg', 'default.json')

        self.time_format = time_format
        self.record_class = "PbsRecord"
        self.load_config(default_config)

    def load_config(self, file_path):
        try:
            with open(file_path, "r") as config_file:
                config = json.load(config_file)

                for key, raw_value in config.items():
                    if "_labels" in key:
                        value = { field : (label.format(self.time_format) if "{" in label else label) for field, label in raw_value.items() }
                    else:
                        value = raw_value

                    if key == "table_format":
                        table_format = {}

                        for format_type, format_str in value.items():
                            try:
                                self.table_format_data[format_type] = self.translate_format(format_str)
                            except AttributeError:
                                self.table_format_data = {format_type : self.translate_format(format_str) }
                    elif key == "long_fields":
                        long_fields = []

                        for field in value:
                            long_fields.append(self.translate_field(field))

                        setattr(self, "long_fields_data", long_fields)

                    if hasattr(self, key):
                        if isinstance(value, dict):
                            for child_key, child_value in value.items():
                                getattr(self, key)[child_key] = child_value
                        else:
                            setattr(self, key, value)
                    else:
                        setattr(self, key, value)
        except JSONDecodeError:
            exit("Error: config file is not valid JSON ({})".format(file_path))
        except FileNotFoundError:
            exit("Error: config file not found at specified path ({})".format(file_path))

        if not hasattr(self, "pbs_log_path"):
            if "PBS_HOME" in os.environ:
                pbs_log_path = "{}/server_priv/accounting".format(os.environ["PBS_HOME"])

                if os.path.isdir(pbs_log_path):
                    self.pbs_log_path = pbs_log_path

        if not hasattr(self, "pbs_log_start"):
            try:
                self.pbs_log_start = sorted(f for f in os.listdir(self.pbs_log_path) if os.path.isfile(os.path.join(self.pbs_log_path, f)))[0]
            except FileNotFoundError:
                exit("Error: log directory nof found ({})".format(self.pbs_log_path))
            except AttributeError:
                pass

    def translate_format(self, format_str):
        new_specs = []

        for format_spec in format_str[1:].split(" {"):
            if ":" in format_spec:
                key, spec = format_spec.split(":", 1)

                if key in self.format_map:
                    new_specs.append("{{{}:{}".format(self.format_map[key], spec))
                else:
                    new_specs.append("{" + format_spec)
            elif format_spec[:-1] in self.format_map:
                new_specs.append("{{{}}}".format(self.format_map[format_spec[:-1]]))
            else:
                new_specs.append("{" + format_spec)

        return " ".join(new_specs)

    def translate_field(self, field):
        if field in self.format_map:
            return self.format_map[field]
        else:
            return field

    def generate_header(self, format_type, custom_format = None, units = "none", divider = True):
        if custom_format:
            data_format = custom_format
        else:
            data_format = self.table_format[format_type]

        header_specs = []
        dividers = {}

        for format_spec in data_format[1:].split(" {"):
            try:
                format_key, format_str = format_spec[:-1].split(":", 1)
            except ValueError:
                header_specs.append("{" + format_spec)
                dividers[format_spec[:-1]] = "-----"
                continue

            if "%" in format_str:
                str_length = len(format(EMPTY_DATETIME, format_str))
                header_specs.append("{{{}:>{str_len}.{str_len}}}".format(format_key, str_len = str_length))
                dividers[format_key] = "-" * str_length
            elif "f" in format_str or "d" in format_str:
                str_length = format_str.translate(str.maketrans("", "", "fd+= ")).split(".")[0]
                header_specs.append("{{{}:{}.{}}}".format(format_key, str_length, str_length.replace(">", "")))
                dividers[format_key] = "-" * int(''.join(c for c in str_length.split(".")[0] if c.isdigit()))
            else:
                header_specs.append("{" + format_spec)
                dividers[format_key] = "-" * int(''.join(c for c in format_str.split(".")[0] if c.isdigit()))

        header_labels = getattr(self, "{}_labels".format(format_type))
        header_format = " ".join(header_specs)

        if units in ("none", "break"):
            formatter = FillFormatter()
            header_units = {}

            for key, value in header_labels.items():
                if "(" in value:
                    label, unit = value.split("(")
                    header_labels[key] = label.rstrip()
                    header_units[key] = "(" + unit

            header_str = header_format.format(**header_labels)

            if units == "break":
                header_str += "\n" + formatter.format(header_format, **header_units)
        else:
            header_str = header_format.format(**header_labels)

        if divider:
            header_str += "\n" + header_format.format(**dividers)

        return header_str

#
## Functions
#

def get_time_bounds(log_start, log_format, period = None, days = 0):
    cur_date  = datetime.datetime.today()

    if period:
        try:
            if '-' in period:
                bounds = [datetime.datetime.strptime(d.split("T")[0], log_format) for
                            d in period.split('-')]
            else:
                bounds = [datetime.datetime.strptime(period.split("T")[0], log_format)] * 2
        except ValueError:
            print("Date range not in a valid format...", file = sys.stderr)
            print("    showing today's jobs instead\n", file = sys.stderr)
            bounds = [cur_date - ONE_DAY, cur_date]
    else:
        bounds = [cur_date - ONE_DAY * int(days), cur_date]

    # Check to make sure bounds fit into range
    log_start = datetime.datetime.strptime(log_start, log_format)

    if bounds[0] < log_start:
        print("Starting date preceeds beginning of logs...", file = sys.stderr)
        print("    using {} instead\n".format(log_start), file = sys.stderr)
        bounds[0] = log_start

    if bounds[1] > cur_date:
        print("Ending date is in the future...", file = sys.stderr)
        print("    using today instead\n", file = sys.stderr)
        bounds[1] = cur_date

    return bounds

def tabular_output(data, fmt_spec, fill_value = "-"):
    formatter = FillFormatter(fill_value = fill_value)
    return formatter.format(fmt_spec, **data)

def list_output(job, fields, labels, format_str, nodes = False):
    print(job.id)

    for field in fields:
        try:
            if "[" in field:
                field_dict, field_key = field.split("[")
                value = getattr(job, field_dict)[field_key[:-1]]
            else:
                value = getattr(job, field)

            if isinstance(value, float):
                print(format_str.format(labels[field], "{:.2f}".format(value)))
            else:
                print(format_str.format(labels[field], value))
        except AttributeError:
            print(format_str.format(labels[field], "N/A"))

    if nodes:
        print(format_str.format("Node List", "{}\n".format(",".join(job.get_nodes()))))
    else:
        print()

def csv_output(job, fields):
    values = []

    for field in fields:
        try:
            if "[" in field:
                field_dict, field_key = field.split("[")
                values.append(str(getattr(job, field_dict)[field_key[:-1]]))
            elif field == "nodelist":
                values.append("+".join(job.get_nodes()))
            else:
                values.append(str(getattr(job, field)))
        except AttributeError:
            values.append("")

    print(",".join(values))

def json_output(job):
    json_dict = {}

    for key, value in job.__dict__.items():
        if not key.startswith("_") and key != "id":
            if isinstance(value, datetime.datetime):
                json_dict[key] = str(value)
            else:
                json_dict[key] = value

    return json.dumps({job.id : json_dict}, indent = 4)

def keep_going(bounds, log_date, reverse = False):
    if reverse:
        return log_date >= bounds[0]
    else:
        return log_date <= bounds[1]

def get_parser():
    # Argument dictionary storage
    help_dict = {   "account"   : "filter jobs by a specific account/project code",
                    "average"   : "print average resource statistics in default/wide mode",
                    "csv"       : "output jobs in csv format",
                    "days"      : "number of days prior to search (default = 0)",
                    "events"    : "list of events to display (E=end, R=requeue)",
                    "filter"    : "specify a freeform filter (--filter=help for more)",
                    "format"    : "use custom format (--format=help for more)",
                    "hosts"     : "only print jobs that ran on specified comma-delimited list of nodes",
                    "json"      : "output jobs in json format",
                    "jobs"      : "one or more job IDs",
                    "list"      : "display untruncated output in list format",
                    "mode"      : "output mode",
                    "name"      : "only print jobs that have the specified job name",
                    "nodes"     : "show list of nodes for each job",
                    "noheader"  : "do not display a header for tabular output",
                    "period"    : "specify time range (YYYYmmdd-YYYYmmdd or YYYYmmdd for a single day)",
                    "queue"     : "filter jobs by a specific queue",
                    "reverse"   : "print jobs in reverse order",
                    "status"    : "only print jobs with specified exit status",
                    "time"      : "display time deltas in seconds, minutes, or hours (default)",
                    "units"     : "add units to tabular or csv headers",
                    "user"      : "filter jobs by a specific user",
                    "wait"      : "show jobs with queue waits above value in minutes",
                    "wide"      : "use wide table columns and show job names" }

    # Define command line arguments
    parser = argparse.ArgumentParser(prog = "qhist", description = qhist_help)

    # Optional arguments
    parser.add_argument("-A", "--account",  help = help_dict["account"])
    parser.add_argument("-a", "--average",  help = help_dict["average"],     action = "store_true")
    parser.add_argument("-c", "--csv",      help = help_dict["csv"],         action = "store_true")
    parser.add_argument("-d", "--days",     help = help_dict["days"],        default = 0)
    parser.add_argument("-e", "--events",   help = help_dict["events"],      default = "E")
    parser.add_argument("-F", "--filter",   help = help_dict["filter"])
    parser.add_argument("-f", "--format",   help = help_dict["format"])
    parser.add_argument("-H", "--hosts",    help = help_dict["hosts"],       nargs = "*", metavar = "HOST")
    parser.add_argument("-J", "--json",     help = help_dict["json"],        action = "store_true")
    parser.add_argument("-j", "--jobs",     help = help_dict["jobs"],        nargs = "*", metavar = "JOBID")
    parser.add_argument("-l", "--list",     help = help_dict["list"],        action = "store_true")
    parser.add_argument("-N", "--name",     help = help_dict["name"],        dest = "jobname")
    parser.add_argument("-n", "--nodes",    help = help_dict["nodes"],       action = "store_true")
    parser.add_argument("--noheader",       help = help_dict["noheader"],    action = "store_true")
    parser.add_argument("-p", "--period",   help = help_dict["period"])
    parser.add_argument("-q", "--queue",    help = help_dict["queue"])
    parser.add_argument("-r", "--reverse",  help = help_dict["reverse"],     action = "store_true")
    parser.add_argument("-s", "--status",   help = help_dict["status"],      dest = "Exit_status")
    parser.add_argument("-t", "--time",     help = help_dict["time"],        default = "h", choices = ["s","m","h","d"])
    parser.add_argument("-U", "--units",    help = help_dict["units"],       action = "store_true")
    parser.add_argument("-u", "--user",     help = help_dict["user"])
    parser.add_argument("-W", "--wait",     help = help_dict["wait"])
    parser.add_argument("-w", "--wide",     help = help_dict["wide"],        action = "store_true")

    return parser

#
## Main code
#

def main():
    my_path = os.path.dirname(__file__)

    # Handle job ID and log path arguments
    parser = get_parser()
    args = parser.parse_args()

    # Load the default configuration settings
    config = QhistConfig(time_format = args.time)

    # There are multiple ways to specify additional custom settings from here on
    # We check them in order of their precedence here
    if "QHIST_SERVER_CONFIG" in os.environ:
        config.load_config(os.environ["QHIST_SERVER_CONFIG"])
    else:
        config_path = os.path.join(my_path, 'cfg', 'server.json')

        if os.path.isfile(config_path):
            config.load_config(config_path)
        elif os.path.isfile("/etc/qhist/server.json"):
            config.load_config("/etc/qhist/server.json")

    # After all of this, we need to check if settings exist
    if not hasattr(config, "pbs_log_path"):
        exit("Error: path to PBS accounting logs not set by config file.")

    # If a custom record type is defined, we should import extensions
    CustomRecord = None

    if config.record_class != "PbsRecord":
        extensions_path = os.path.join(my_path, "extensions")
        extension_files = glob(extensions_path + "/*.py")

        if extension_files:
            sys.path.append(os.path.join(my_path, "extensions"))

            for extension_file in extension_files:
                extension = re.search(".*/(.*).py", extension_file).group(1)

                try:
                    CustomRecord = importlib.import_module(extension).__getattribute__(config.record_class)
                    break
                except AttributeError:
                    pass

        if not CustomRecord:
            exit("Error: given custom record class not found in code extensions ({})".format(config.record_class))

    # Long-form help
    if args.format == "help":
        print(format_help)

        for key in ["id", "short_id"] + sorted(config.format_map):
            print("    {}".format(key))

        print()
        sys.exit()
    elif args.filter == "help":
        print(filter_help)

        for key in sorted(k for k in config.format_map if k not in ("end", "start", "nodelist")):
            print("    {}".format(key))

        print()
        sys.exit()

    # Time format option
    if args.time == "h":
        time_divisor = 3600.0
    elif args.time == "m":
        time_divisor = 60.0
    elif args.time == "s":
        time_divisor = 1.0
    elif args.time == "d":
        time_divisor = 86400.0

    # Time bounds, if set
    time_filters = None

    if args.period and "T" in args.period:
        if "-" not in args.period:
            print("Warning: Time only valid when specifying period range. Ignoring...", file = sys.stderr)
        else:
            time_filters = []
            input_format = "%Y%m%dT%H%M%S"

            for bound in args.period.split("-"):
                time_filters.append(datetime.datetime.strptime(bound, input_format[0:(len(bound) - 2)]))

    # Collect filter parameters
    if args.jobs:
        if len(args.jobs) == 1:
            id_filter = [job.split(".")[0] if "." in job else job for job in args.jobs[0].split(",")]
        else:
            id_filter = [job.split(".")[0] if "." in job else job for job in args.jobs]
    else:
        id_filter = None

    if args.hosts:
        if len(args.hosts) == 1:
            host_filter = args.hosts[0].split(",")
        else:
            host_filter = args.hosts
    else:
        host_filter = None

    data_filters = []
    ops = OrderedDict([("==",    operator.eq),
                       ("~=",    operator.ne),
                       ("<=",    operator.le),
                       (">=",    operator.ge),
                       ("=",     operator.eq),
                       ("<",     operator.lt),
                       (">",     operator.gt)])

    for arg_filter in ("account", "jobname", "queue", "user", "Exit_status"):
        filter_value = getattr(args, arg_filter)

        if filter_value:
            if filter_value[0] == "~":
                data_filters.append((False, operator.ne, arg_filter, filter_value[1:]))
            else:
                data_filters.append((False, operator.eq, arg_filter, filter_value))

    if args.wait:
        if args.wait[0] == "~":
            data_filters.append((False, operator.le, "waittime", float(args.wait[1:]) / 60))
        else:
            data_filters.append((False, operator.gt, "waittime", float(args.wait) / 60))

    if args.filter:
        for fexpr in args.filter.split(";"):
            for op in ops:
                if op in fexpr:
                    if fexpr[0] == "~":
                        negation = True
                        field, match = fexpr[1:].split(op)
                    else:
                        negation = False
                        field, match = fexpr.split(op)

                    data_filters.append((negation, ops[op], config.translate_field(field), match))
                    break

    if args.list or args.csv or args.json:
        max_width = 0

        if args.format:
            field_list = args.format.split(",")
            labels = {config.translate_field(f) : config.wide_labels[f] for f in field_list}
            fields = [config.translate_field(f) for f in field_list]
        else:
            labels = {config.translate_field(f) : config.wide_labels[f] for f in config.long_fields}
            fields = config.long_fields_data

        if args.list:
            for l in labels.values():
                label_width = len(l)

                if label_width > max_width:
                    max_width = label_width

            list_format = "   {:" + str(max_width) + "} = {}"

        if args.list or args.json:
            try:
                fields.remove("id")
            except ValueError:
                pass
        else:
            if args.nodes and "nodelist" not in fields:
                fields.append("nodelist")

            if not args.noheader:
                if args.units:
                    print(",".join(labels[f] for f in fields))
                else:
                    print(",".join(labels[f].split('(')[0].rstrip() for f in fields))
    else:
        format_type = "default"

        if args.units:
            units = "break"
        else:
            units = "none"

        if args.wide:
            format_type = "wide"

        if args.format:
            if not args.noheader:
                print(config.generate_header(format_type, custom_format = args.format, units = units))

            table_format = config.translate_format(args.format)
        else:
            if not args.noheader:
                print(config.generate_header(format_type, units = units))

            table_format = config.table_format_data[format_type]

        if args.average:
            num_jobs = 0
            averages = {"Resource_List" : {}, "resources_used" : {}}
            avg_spec = []

            for field in ("ncpus", "ngpus", "nodect", "walltime", "mem"):
                averages["Resource_List"][field] = 0.0

            for field in ("cpupercent", "walltime", "mem", "avgcpu"):
                averages["resources_used"][field] = 0.0

            averages_format = re.sub(r"(\d+)d", r"\1.2f", table_format)

    # Begin iterating over log data within specified time bounds
    bounds = get_time_bounds(config.pbs_log_start, config.pbs_date_format, period = args.period, days = args.days)

    if args.reverse:
        log_date = bounds[1]
    else:
        log_date = bounds[0]

    while keep_going(bounds, log_date, args.reverse):
        data_date = datetime.datetime.strftime(log_date, config.pbs_date_format)
        data_file = os.path.join(config.pbs_log_path, data_date)
        jobs = get_pbs_records(data_file, CustomRecord, True, args.events,
                               id_filter, host_filter, data_filters, time_filters,
                               args.reverse, time_divisor)

        if args.list:
            for job in jobs:
                list_output(job, fields, labels, list_format, nodes = args.nodes)
        elif args.csv:
            for job in jobs:
                csv_output(job, fields)
        elif args.json:
            first_job = True

            print("{")
            print('    "timestamp":{},'.format(int(datetime.datetime.today().timestamp())))
            print('    "Jobs":{')

            for job in jobs:
                if not first_job:
                    print(",")

                print(textwrap.indent(json_output(job)[2:-2], "    "), end = "")
                first_job = False

            print("\n    }\n}")
        elif args.nodes:
            if args.average:
                for job in jobs:
                    if '[]' not in job.id:
                        for category in averages:
                            for field in averages[category]:
                                averages[category][field] += getattr(job, category)[field]

                        num_jobs += 1

                    print("{}\n    {}".format(tabular_output(vars(job), table_format), ",".join(job.get_nodes())))
            else:
                for job in jobs:
                    print("{}\n    {}".format(tabular_output(vars(job), table_format), ",".join(job.get_nodes())))
        else:
            if args.average:
                for job in jobs:
                    if '[]' not in job.id:
                        for category in averages:
                            for field in averages[category]:
                                averages[category][field] += getattr(job, category)[field]

                        num_jobs += 1
                    print(tabular_output(vars(job), table_format))
            else:
                for job in jobs:
                    print(tabular_output(vars(job), table_format))

        if args.reverse:
            log_date -= ONE_DAY
        else:
            log_date += ONE_DAY

    if args.average and num_jobs > 0:
        for category in averages:
            for field in averages[category]:
                averages[category][field] /= num_jobs

        print("\nAverages across {} jobs:\n".format(num_jobs))

        if not args.noheader:
            if args.format:
                print(config.generate_header(format_type, custom_format = args.format, units = units))
            else:
                print(config.generate_header(format_type, units = units))

        print(tabular_output(averages, averages_format))

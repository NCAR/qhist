import sys, os, argparse, datetime, signal, string, _string, json

from .pbs_history import PbsRecord, get_pbs_records

# Use default signal behavior on system rather than throwing IOError
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Constants
ONE_DAY = datetime.timedelta(days = 1)
EMPTY_DATETIME = datetime.datetime(1,1,1)

# Argument dictionary storage
arg_help    = { "account"   : "filter jobs by a specific account/project code",
                "average"   : "print average resource statistics in standard view",
                "csv"       : "output jobs in csv format",
                "days"      : "number of days prior to search (default = 0)",
                "events"    : "list of events to display (E=end, R=requeue, S=shrink)",
                "filter"    : "filter the output using criteria on any field",
                "format"    : "use custom format (--format=help for more)",
                "hosts"     : "only print jobs that ran on specified comma-delimited list of nodes",
                "json"      : "output jobs in json format",
                "jobs"      : "one or more job IDs",
                "list"      : "display untruncated output in list format",
                "mode"      : "output mode",
                "name"      : "only print jobs that have the specified job name",
                "nodes"     : "show list of nodes for each job",
                "noheader"  : "do not display a header for tabular output",
                "period"    : "search over specific date range (YYYYMMDD-YYYYMMDD or YYYYMMDD for a single day)",
                "queue"     : "filter jobs by a specific queue",
                "retcode"   : "only print jobs with return code (or prefix with x to exclude)",
                "sort"      : "sort by any field (--sort=help for more)",
                "time"      : "display time deltas in seconds, minutes, or hours (default)",
                "user"      : "filter jobs by a specific user",
                "wait"      : "show jobs with queue waits above value (mins)",
                "wide"      : "use wide table columns and show job names" }

# Long-form help statements
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
This option allows you to filter job data by a comma-delimited list of fields
and expressions. Note that '<', '>', and '!'  will be interpreted by the shell
and thus you should encapsulate your expression in quotes.

Examples:
    qhist --filter="cputype!=milan,ompthreads>1"

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
                    obj = obj[i]

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
    def __init__(self, **kwargs):
        if "file" in kwargs:
            self.load_config(kwargs["file"])

    def load_config(self, file_path):
        with open(file_path, "r") as config_file:
            config = json.load(config_file)

            for key, value in config.items():
                if key == "table_format":
                    table_format = {}

                    for format_type, format_str in config[key].items():
                        table_format[format_type] = self.translate_format(format_str)

                    setattr(self, "table_format_data", table_format)
                elif key == "long_fields":
                    long_fields = []
                    
                    for field in config["long_fields"]:
                        long_fields.append(self.translate_field(field))

                    setattr(self, "long_fields_data", long_fields)

                setattr(self, key, value)

    def translate_format(self, format_str):
        new_specs = []

        for format_spec in format_str.split():
            if ":" in format_spec:
                key, spec = format_spec.split(":", 1)

                if key[1:] in self.format_map:
                    new_specs.append("{{{}:{}".format(self.format_map[key[1:]], spec))
                else:
                    new_specs.append(format_spec)
            elif format_spec[1:-1] in self.format_map:
                new_specs.append("{{{}}}".format(self.format_map[format_spec[1:-1]]))
            else:
                new_specs.append(format_spec)

        return " ".join(new_specs)

    def translate_field(self, field):
        if field in self.format_map:
            return self.format_map[field]
        else:
            return field

    def generate_header(self, format_type, custom_format = None, unit_line = False, divider = True):
        if custom_format:
            data_format = custom_format
        else:
            data_format = self.table_format[format_type]

        header_specs = []
        dividers = {}

        for format_spec in data_format.split():
            try:
                format_key, format_str = format_spec[1:-1].split(":", 1)
            except ValueError:
                header_specs.append(format_spec)
                dividers[format_spec[1:-1]] = "-----"
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
                header_specs.append(format_spec)
                dividers[format_key] = "-" * int(''.join(c for c in format_str.split(".")[0] if c.isdigit()))

        header_labels = getattr(self, "{}_labels".format(format_type))
        header_format = " ".join(header_specs)

        if unit_line:
            formatter = FillFormatter()
            header_units = {}

            for key, value in header_labels.items():
                if "(" in value:
                    label, units = value.split("(")
                    header_labels[key] = label.rstrip()
                    header_units[key] = "(" + units

            header_str = header_format.format(**header_labels)
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
                bounds = [datetime.datetime.strptime(d, log_format) for
                            d in period.split('-')]
            else:
                bounds = [datetime.datetime.strptime(period, log_format)] * 2
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

def tabular_output(job, fmt_spec, fill_value = "-"):
    formatter = FillFormatter(fill_value = fill_value)
    return formatter.format(fmt_spec, **vars(job))

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

#
## Main code
#

def main():
    # Load config for this system
    config = QhistConfig()
    qhist_root = os.path.dirname(os.path.realpath(__file__))
    
    for file in ("default", os.environ["NCAR_HOST"]):
        config_path = "{}/../etc/{}.json".format(qhist_root, file)

        if os.path.isfile(config_path):
            config.load_config(config_path)

    # Define command line arguments
    parser = argparse.ArgumentParser(prog = "qhist",                    
                description = "Search PBS logs for finished jobs.")

    # Optional arguments
    parser.add_argument("-A", "--account",  help = arg_help["account"])
    parser.add_argument("-a", "--average",  help = arg_help["average"], action = "store_true")
    parser.add_argument("-c", "--csv",      help = arg_help["csv"],     action = "store_true")
    parser.add_argument("-d", "--days",     help = arg_help["days"],    default = 0)
    parser.add_argument("-e", "--events",   help = arg_help["events"],  default = "E")
    parser.add_argument("-F", "--filter",   help = arg_help["filter"])
    parser.add_argument("-f", "--format",   help = arg_help["format"])
    parser.add_argument("-H", "--hosts",    help = arg_help["hosts"])
    parser.add_argument("-j", "--json",     help = arg_help["json"],    action = "store_true")
    parser.add_argument("joblist",          help = arg_help["jobs"],    nargs = "*", metavar = "jobid")
    parser.add_argument("-l", "--list",     help = arg_help["list"],    action = "store_true")
    parser.add_argument("-N", "--name",     help = arg_help["name"],    dest = "jobname")
    parser.add_argument("-n", "--nodes",    help = arg_help["nodes"],   action = "store_true")
    parser.add_argument("--noheader",       help = arg_help["noheader"],action = "store_true")
    parser.add_argument("-p", "--period",   help = arg_help["period"])
    parser.add_argument("-q", "--queue",    help = arg_help["queue"])
    parser.add_argument("-r", "--retcode",  help = arg_help["retcode"], dest = "Exit_status")
    parser.add_argument("-s", "--sort",     help = arg_help["sort"],    default = "finish")
    parser.add_argument("-t", "--time",     help = arg_help["time"],    default = "h")
    parser.add_argument("-u", "--user",     help = arg_help["user"])
    parser.add_argument("-W", "--wait",     help = arg_help["wait"],    default = "-1")
    parser.add_argument("-w", "--wide",     help = arg_help["wide"],    action = "store_true")

    # Handle job ID and log path arguments
    args = parser.parse_args()

    # Long-form help
    if args.format == "help":
        print(format_help)

        for key in sorted(config.format_map):
            print("    {}".format(key))

        print()
        sys.exit()
    elif args.filter == "help":
        print(filter_help)

        for key in sorted(k for k in config.format_map if k not in ("end", "start")):
            print("    {}".format(key))
        
        print()
        sys.exit()

    # Collect filter parameters
    filters = {}

    for arg_filter in ("account", "jobname", "queue", "user", "Exit_status", "hosts", "joblist"):
        filter_value = getattr(args, arg_filter)
    
        if filter_value:
            filters[arg_filter] = filter_value

    if "joblist" in filters:
        filters["joblist"] = ["{}.{}".format(job, config.pbs_server) if "." not in job else job for job in filters["joblist"]]

    if "hosts" in filters:
        filters["hosts"] = filters["hosts"].split(",")

    # Begin iterating over log data within specified time bounds
    bounds = get_time_bounds(config.pbs_log_start, config.pbs_date_format, period = args.period, days = args.days)
    log_date = bounds[0]

    if args.list or args.csv or args.json:
        max_width = 0

        if args.format:
            field_list = args.format.split(",")
            labels = {config.translate_field(f) : config.wide_labels[f] for f in field_list}
            fields = (config.translate_field(f) for f in field_list)
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
            fields.remove("id")
    else:
        format_type = "default"
        
        if args.wide:
            format_type = "wide"

        if args.format:
            if not args.noheader:
                print(config.generate_header(format_type, custom_format = args.format))
            table_format = config.translate_format(args.format)
        else:
            if not args.noheader:
                print(config.generate_header(format_type))
            table_format = config.table_format_data[format_type]

    while log_date <= bounds[1]:
        data_date = datetime.datetime.strftime(log_date, config.pbs_date_format)
        data_file = os.path.join(config.pbs_log_path, data_date)
        jobs = get_pbs_records(data_file, process = True, record_filter = args.events, data_filters = filters)

        if args.list:
            for job in jobs:
                list_output(job, fields, labels, list_format, nodes = args.nodes)
        elif args.csv:
            if args.nodes and "nodelist" not in fields:
                fields.append("nodelist")

            if not args.noheader:
                print(",".join(labels[f] for f in fields))

            for job in jobs:
                csv_output(job, fields)
        elif args.json:
            #TODO
            for job in jobs:
                json_output(job)
        elif args.nodes:
            for job in jobs:
                print("{}\n    {}".format(tabular_output(job, table_format, ",".join(job.get_nodes()))))
        else:
            for job in jobs:
                print(tabular_output(job, table_format))

        log_date += ONE_DAY

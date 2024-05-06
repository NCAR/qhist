#!/usr/bin/env python3

import sys, argparse, json
import signal
import requests

signal.signal(signal.SIGPIPE, signal.SIG_DFL)

arg_help    = { "account"   : "filter jobs by a specific account/project code",
                "average"   : "print average resource statistics in standard view",
                "brief"     : "only output the PBS job IDs",
                "csv"       : "print all processed fields in csv format",
                "days"      : "number of days prior to search (default = 0)",
                "events"    : "list of events to display (E=end, R=requeue, S=shrink)",
                "format"    : "use custom format (--format=help for more)",
                "infile"    : "import past query from a specified pickle file",
                "job"       : "only display output for a specific job ID",
                "list"      : "display untruncated output in list format",
                "momlist"   : "only print jobs that ran on specified plus-delimited list of nodes",
                "name"      : "only print jobs that have the specified job name",
                "nodes"     : "show list of nodes for each job",
                "outfile"   : "export results to a pickle object at specified path",
                "period"    : "search over specific date range (YYYYMMDD-YYYYMMDD or YYYYMMDD for a single day)",
                "queue"     : "filter jobs by a specific queue",
                "retcode"   : "only print jobs with return code (or prefix with x to exclude)",
                "sort"      : "sort by any field (--sort=help for more)",
                "time"      : "display time deltas in seconds, minutes, or hours (default)",
                "timefmt"   : "force use of time format (default, wide, or long)",
                "user"      : "filter jobs by a specific user",
                "wait"      : "show jobs with queue waits above value (mins)",
                "wide"      : "use wide table columns and show job names" }

# Long-form help statements
fmt_help = """
This option allows you to specify a custom format. This
setting's behavior depends on which mode you are using:

For default and wide behavior, enter a string containing
Python's format syntax. Modulo and f-string formatting is
not supported. All values will be strings, so only string
formatting should be used. For list and csv modes, a
comma-delimited string with field names is the expected
input. Brief and export modes are not compatible with this
option.

Note that the job id is always included as the first field.
The following variables are available:
"""

sort_help = """
This option allows you to sort the output by a specific
field. You can also specify ascending or descending order
by adding + or - to the end of the field respectively. The
following variables are available:
"""
def main():
    # Define command line arguments
    parser = argparse.ArgumentParser(prog = "qhist",                    
                description = "Search PBS logs for finished jobs.")

    # Optional arguments
    parser.add_argument("-A", "--account", help = arg_help["account"])
    parser.add_argument("-a", "--average", help = arg_help["average"],
            action = "store_true")
    parser.add_argument("-b", "--brief", help = arg_help["brief"],
            action = "store_true")
    parser.add_argument("-c", "--csv", help = arg_help["csv"],
            action = "store_true")
    parser.add_argument("-d", "--days", help = arg_help["days"],
            default = 0)
    parser.add_argument("-e", "--events", help = arg_help["events"],
            default = "E")
    parser.add_argument("-f", "--format", help = arg_help["format"])
    parser.add_argument("-i", "--infile", help = arg_help["infile"])
    parser.add_argument("-j", "--job", help = arg_help["job"])
    parser.add_argument("-l", "--list", help = arg_help["list"],
            action = "store_true")
    parser.add_argument("-m", "--momlist", help = arg_help["momlist"])
    parser.add_argument("-N", "--name", help = arg_help["name"])
    parser.add_argument("-n", "--nodes", help = arg_help["nodes"],
            action = "store_true")
    parser.add_argument("-o", "--outfile", help = arg_help["outfile"])
    parser.add_argument("-p", "--period", help = arg_help["period"])
    parser.add_argument("-q", "--queue", help = arg_help["queue"])
    parser.add_argument("-r", "--retcode", help = arg_help["retcode"])
    parser.add_argument("-s", "--sort", help = arg_help["sort"],
            default = "finish")
    parser.add_argument("-t", "--time", help = arg_help["time"],
            default = "h")
    parser.add_argument("-T", "--timefmt", help = arg_help["timefmt"])
    parser.add_argument("-u", "--user", help = arg_help["user"])
    parser.add_argument("-W", "--wait", help = arg_help["wait"],
            default = "-1")
    parser.add_argument("-w", "--wide", help = arg_help["wide"],
            action = "store_true")

          # Handle job ID and log path arguments
    args = parser.parse_args()

    if args.format == "help":
        print(fmt_help)
        for key in sorted(qhs_core["long_labels"]):
            print("    {}".format(key))
       
        print("\nExamples:")
        print("    qhist --format='{account:10} {reqmem:8} {memory:8}'")
        print("    qhist --list --format='account,reqmem,memory'\n")
        sys.exit()

    if args.sort == "help":
        print(sort_help)
        for key in sorted(qhs_core["long_labels"]):
            print("    {}".format(key))
        print()
        sys.exit()

    resp = requests.get("http://127.0.0.1:5000",json=json.dumps(vars(args)))
    resp = json.loads(resp.text)
    if len(resp['err']) > 0:
        print(resp['err'], file = sys.stderr)
    if len(resp['msg']) > 0:
        print(resp['msg'])


if '__main__' == __name__:
    main()


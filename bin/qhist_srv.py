#!/usr/bin/env python3

import os, subprocess, copy, re, json
import signal, time, collections, operator, datetime
import copy
from flask import Flask, request

app = Flask(__name__)


# Use default signal behavior on system rather than throwing IOError
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Core qhist settings
qhist_root = os.path.dirname(os.path.realpath(__file__))
# make QHS_CORE and QHS_SYS constants so they aren't modified by each request
with open(qhist_root + "/../etc/core.json",'r') as cf:
    QHS_CORE = json.load(cf)

# System settings
with open(qhist_root + "/../etc/{}.json".format(os.environ["NCAR_HOST"]), 'r') as sf:
    QHS_SYS = json.load(sf)

# Operational constants
cur_date    = datetime.datetime.today()
one_day     = datetime.timedelta(days = 1)
one_ms      = datetime.timedelta(milliseconds = 1)
mem_conv    = { "kb"    : (1.0 / 1048576),
                "mb"    : (1.0 / 1024),
                "gb"    : 1,
                "tb"    : 1024 }
tt_events   = str.maketrans("ue", "SS")
node_regex  = re.compile('\(([^:]*)')

# Argument dictionary storage

# Silly hack to convert a dict to an obj
# used to convert the dict sent from client back into something that looks
# like an argparse

@app.route("/")
def main():
    qhs_sys = copy.deepcopy(QHS_SYS)
    qhs_core = copy.deepcopy(QHS_CORE)
    resp = {'err': "", 'msg': ""}

    #
    ## FUNCTION DEFINITIONS
    #
    
    def get_log_data(logs, events, include, exclude):
        out_data    = []
        grep_cmd    = ["grep", "-a", "-h"]
    
        for e in qhs_core["job_events"]:
            if e in events:
                for sstr in qhs_core["job_events"][e]:
                    grep_cmd.extend(["-e", ";{};".format(sstr)])
    
        with subprocess.Popen(grep_cmd + logs, stdout = subprocess.PIPE,
                stderr = subprocess.DEVNULL, universal_newlines = True) as p:
            for entry in p.stdout:
                in_pass = all(item in entry for item in include)
                ex_pass = not any(item in entry for item in exclude)
    
                if in_pass and ex_pass:
                    out_data.append(entry)
            
            while p.poll() is None:
                time.sleep(0.1)
    
            return p.returncode, out_data
    
    def get_time_bounds(args):
        err = ""
        if args["period"]:
            try:
                if '-' in args["period"]:
                    bounds = [datetime.datetime.strptime(d, qhs_core["pbs_fmt"]) for
                                d in args["period"].split('-')]
                else:
                    bounds = [datetime.datetime.strptime(args["period"], qhs_core["pbs_fmt"])] * 2
            except ValueError:
                err += "Date range not in a valid format..." + "\n"
                err += "    showing today's jobs instead" + "\n"
                bounds = [cur_date - one_day, cur_date]
        else:
            bounds = [cur_date - one_day * int(args["days"]), cur_date]
        
        # Check to make sure bounds fit into range
        log_start = datetime.datetime.strptime(qhs_sys["log_start"], qhs_core["pbs_fmt"])
        
        if bounds[0] < log_start:
            err += "Starting date preceeds beginning of logs..." + "\n"
            err += "    using {} instead\n".format(qhs_sys["log_start"]) + "\n"
            bounds[0] = log_start
        
        if bounds[1] > cur_date:
            err += "Ending date is in the future..." + "\n"
            err += "    using today instead\n" + "\n"
            bounds[1] = cur_date
        
        return (bounds, err)
    
    def format_job_times(job, fmt_type):
        time_fmt    = qhs_core["time_fmt"][fmt_type]
        time_fields = qhs_core["time_fields"][fmt_type]
    
        for tv in qhs_core["time_vars"]:
            if job[tv] != '-':
                job[tv] = time_fmt.format(*operator.attrgetter(*time_fields)(job[tv]))
    
    def format_job_numerics(job, fmt_type, delta_unit):
        for dv in qhs_core["delta_vars"]:
            if job[dv] != '-':
                if delta_unit == 'm':
                    job[dv] = job[dv] / 60.0
                elif delta_unit == 'h':
                    job[dv] = job[dv] / 3600.0
    
        for dv in qhs_core[fmt_type]:
            if job[dv] != '-':
                job[dv] = qhs_core[fmt_type][dv].format(job[dv])
    
    def mixcomp(item):
        if isinstance(item, str):
            return (0, item)
        else:
            return (1, item)
    
    def sort_log(jobs, method):
        err = ""
        if method[-1] in ['+','-']:
            sort_ascend = method[-1] == '+'
            method      = method[:-1]
        else:
            sort_ascend = True
    
        # Get sort index based on method
        if method in jobs[0]:
            jobs.sort(key = lambda d: mixcomp(d[method]), reverse = sort_ascend)
        else:
            err += "Error: sorting method {} not recognized...".format(method) + "\n"
            err += "       using finish time instead\n" + "\n"
        return err
    
    def process_job(job_data):
        for v in qhs_core["time_vars"][:-1]:
            try:
                job_data[v] = datetime.datetime.fromtimestamp(float(job_data[v]))
            except ValueError:
                pass
        
        for v in ["walltime","elapsed"]:
            try:
                job_data[v] = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(job_data[v].split(':')))
            except ValueError:
                pass
    
        try:
            job_data["memory"] = float(job_data["memory"][:-2]) * mem_conv[job_data["memory"][-2:]]
        except KeyError:
            job_data["memory"] = 0.0
        except ValueError:
            if job_data["memory"] != '-':
                job_data["memory"] = 0.0
    
        if job_data["reqmem"] != '-':
            try:
                job_data["reqmem"] = float(job_data["reqmem"][:-2]) * mem_conv[job_data["reqmem"][-2:]]
            except (KeyError, ValueError):
                job_data["reqmem"] = 0.0
    
        for v in ["numnodes","numcpus","energy-node","energy-cpu","energy-gpu0","energy-gpu1","energy-gpu2","energy-gpu3","energy-ram"]:
            try:
                job_data[v] = int(job_data[v])
            except ValueError:
                pass
    
        for v in ["mpiprocs","ompthreads","numgpus"]:
            if job_data[v] != '-':
                job_data[v] = int(job_data[v])
    
        try:
            job_data["avgcpu"] = float(job_data["avgcpu"]) / job_data["numcpus"]
        except ZeroDivisionError:
            job_data["avgcpu"] = '-'
        except ValueError:
            pass
    
        job_data["account"]     = job_data["account"].replace('"','')
        job_data["nodelist"]    = '+'.join(node_regex.findall(job_data["nodelist"]))
    
        return job_data
    
    def process_log(log_data, events, wait_limit):
        err = ""
        jobs        = []
        rproto      = dict(zip(qhs_core["long_labels"].keys(), ['-'] * len(qhs_core["long_labels"])))
        
        for entry in log_data:
            records = rproto.copy()
            rt, rec_type, records["id"], job_data = entry.split(';', 3)[:4]
    
            try:
                for item in job_data.split():
                    param, content = item.split('=', 1)
                    
                    if param in qhs_core["raw_conv"]:
                        records[qhs_core["raw_conv"][param]] = content
            
                records["waittime"] = int(records["start"]) - int(records["eligible"])
                
                if records["waittime"] > wait_limit:
                    # Use explicit splicing for speed
                    records["finish"]   = datetime.datetime(int(rt[6:10]),int(rt[0:2]),int(rt[3:5]),int(rt[11:13]),int(rt[14:16]),int(rt[17:19]))
                    records["type"]     = rec_type.translate(tt_events)
                    
                    if rec_type == 'E':
                        records["finish"] += one_ms
    
                    jobs.append(records)
            except ValueError as e:
                err += "Job {} has bad data and cannot be processed; skipping ...".format(records["id"]) + "\n"
        
        # Check if item is already in jobs (requeues result in dups)
        if "requeue" in events:
            jobsub  = [(j["id"],j["start"],j["finish"],j["type"]) for j in jobs]
            dups    = collections.defaultdict(list)
            dupinds = []
    
            for i, e in enumerate(jobsub):
                dups[e].append(i)
    
            for d in dups.items():
                dupinds += d[1][1:]
    
            for n in sorted(dupinds, reverse = True):
                del jobs[n]
    
        # Process fields from all jobs in list
        jobs = list(map(process_job, jobs))
    
        return (jobs, err)
    
    def print_list(jobs, my_fields, job_stats):
        msg = ""
        labels      = [qhs_core["long_labels"][f] for f in my_fields]
        len_max     = max(len(item) for item in labels)
        fmt_str     = "   {:" + str(len_max) + "} {} {}"
    
        if job_stats:
            job_stats["id"] = "Averages Weighted by Job Cost"
            jobs.append(job_stats)
    
        for record in jobs:
            for l, r in zip(labels, [record[f] for f in my_fields]):
                if l == "Job ID":
                    msg += r + "\n"
                else:
                    msg += fmt_str.format(l, '=', r) + "\n"
            
            return msg
    
    def print_table(jobs, my_fmt):
        msg = ""
        for record in jobs:
            msg += my_fmt.format(**record) + "\n"
        return msg
    
    def compute_stats(jobs):
        job_totals  = {f : 0.0 for f in qhs_core["avg_fields"]}
        job_stats   = {f : '-' for f in qhs_core["long_labels"]}
        wght_sum    = 0.0
    
        for job in jobs:
            if "[]" not in job["id"]:
                weight      =   job["numnodes"] * job["elapsed"]
                wght_sum    +=  weight
    
                for f in qhs_core["avg_fields"]:
                    if job[f] != '-':
                        job_totals[f] += job[f] * weight
        
        for f in qhs_core["avg_fields"]:
            job_stats[f] = job_totals[f] / wght_sum
    
        job_stats["id"] = "Average"
    
        return job_stats
    
#
## MAIN PROGRAM EXECUTION
#

    args = json.loads(request.get_json())

    if args["format"]:
        # Make sure user formats are valid
        try:
            test = args["format"].format(**qhs_core["long_labels"])
        except KeyError as e:
            resp['err'] += "Fatal: custom format key not valid ({})\n".format(e) + "\n"
            return json.dumps(resp)

    # Collect search terms
    include = []
    exclude = []
    
    if args["account"]:
        include += ['account="{}" '.format(args["account"])]

    if args["user"]:
        include += ["user={} ".format(args["user"])]
    
    if args["queue"]:
        include += ["queue={} ".format(args["queue"])]

    if args["name"]:
        include += ["jobname={} ".format(args["name"])]
    
    if args["job"]:
        include += [";{}".format(args["job"])]
    
    if args["momlist"]:
        args["momlist"] = ["{}:".format(mom) for mom in args["momlist"].split('+')]
        include += args["momlist"]

    if args["timefmt"] and args["timefmt"] not in qhs_core["time_fmt"]:
        resp['err'] += "Error: invalid time format option (use default, wide, or long)..." + "\n"
        resp['err'] += "       using standard method instead\n" + "\n"
        args["timefmt"] = None
    
    if args["timefmt"]:
        qhs_sys["table_fmt"] = {k : v.format(qhs_core["time_table"][args["timefmt"]]) for k, v in qhs_sys["table_fmt"].items()}
    else:
        qhs_sys["table_fmt"] = {k : v.format(qhs_core["time_table"][k]) for k, v in qhs_sys["table_fmt"].items()}
    
    if args["retcode"]:
        if args["retcode[0]"] == 'x':
            exclude += ["Exit_status={} ".format(args["retcode"][1:])]
        else:
            include += ["Exit_status={} ".format(args["retcode"])]

    # Convert wait time to seconds
    args["wait"] = int(args["wait"]) * 60

    # Multiple events and brief mode don't make sense
    if args["events"] != "E" and args["brief"]:
        resp['err'] += "Error: multiple events cannot be specified in brief mode..." + "\n"
        resp['err'] += "       showing end records only\n" + "\n"
        args["events"] = "E"

    delta_fmt = "{:0.2f}"

    if args["time"] in ['s',"secs","seconds"]:
        args["time"] = 's'
        delta_fmt = "{:d}"
    elif args["time"] in ['m',"mins","minutes"]:
        args["time"] = 'm'
    else:
        if args["time"] not in ['h',"hrs","hours"]:
            resp['err'] += "Error: Time unit {} not recognized...".format(args["time"]) + "\n"
            resp['err'] += "       using hours instead\n" + "\n"

        args["time"] = 'h'

    for dv in qhs_core["delta_vars"]:
        qhs_core["long_labels"][dv]     = qhs_core["long_labels"][dv].format(args["time"])
        qhs_core["short_labels"][dv]    = qhs_core["short_labels"][dv].format(args["time"])
        qhs_core["data_fmt"][dv]        = delta_fmt

    (bounds, tmp_err)      = get_time_bounds(args)
    resp['err'] += tmp_err
    loop_date   = bounds[0]
    pbs_logs    = []
    jobs        = []
    
    while loop_date <= bounds[1]:
        pbs_date = datetime.datetime.strftime(loop_date, qhs_core["pbs_fmt"])
        pbs_logs.append(os.path.join(qhs_sys["pbs_path"], pbs_date))
        loop_date += one_day

    status, log_data = get_log_data(pbs_logs, args["events"], include, exclude)
    
    if status > 1:
        resp['err'] += "Warning: some PBS log files could not be accessed for specified time period" + "\n"

    if len(log_data) > 0:
        (jobs, tmp_err) = process_log(log_data, args["events"], args["wait"])
        resp['err'] += tmp_err

    if len(jobs) > 0:
        if args["brief"]:
            resp['err'] += sort_log(jobs, "id")
            
            for job in jobs:
                resp['msg'] += job["id"].split('.')[0] + "\n"
        else:
            resp['err'] += sort_log(jobs, args["sort"])

            if args["average"]:
                job_stats = compute_stats(jobs)
                format_job_numerics(job_stats, "avg_fmt", args["time"])
            else:
                job_stats = None

            if args["list"] or args["csv"]:
                fmt_type = args["timefmt"] or "long"

                if args["format"]:
                    my_fields = args["format"].split(',')
                else:
                    my_fields = qhs_sys["long_fields"]
                
                for job in jobs:
                    format_job_times(job, fmt_type)
                    format_job_numerics(job, "data_fmt", args["time"])

                if ',' in args["events"]:
                    my_fields = ["id","type"] + my_fields
                else:
                    my_fields = ["id"] + my_fields
    
                if args["list"]:
                    resp['msg'] += print_list(jobs, my_fields, job_stats)
                else:
                    args["nodes"]  = False
                    my_fmt      = ("{{{}}},"*len(my_fields))[:-1].format(*my_fields)

                    if args["average"]:
                        jobs.append(job_stats)

                    resp['msg'] += my_fmt.format(**qhs_core["long_labels"])
                    resp['msg'] += print_table(jobs, my_fmt)
            else:
                # Process job names and get max length for formatting
                if args["wide"]:
                    fmt_type    = "wide"
                    my_labels   = qhs_core["long_labels"]
                else:
                    fmt_type    = "default"
                    my_labels   = qhs_core["short_labels"]
                    
                if args["format"]:
                    my_fmt  = args["format"]
                else:
                    my_fmt  = qhs_sys["table_fmt"][fmt_type]
                
                if ',' in args["events"]:
                    my_fmt = "{type:4.4} " + my_fmt
   
                if args["nodes"]:
                    my_fmt = my_fmt + "\n    {nodelist}"

                if args["timefmt"]:
                    fmt_type = args["timefmt"]

                for job in jobs:
                    job["id"] = job["id"].split('.')[0]
                    format_job_times(job, fmt_type)
                    format_job_numerics(job, "data_fmt", args["time"])

                my_fmt      = "{id:8.8}  " + my_fmt
                my_header   = my_fmt.format(**my_labels)
                
                resp['msg'] += my_header + "\n"
                resp['msg'] += print_table(jobs, my_fmt)

                if args["average"]:
                    resp['msg'] += "\n"
                    resp['msg'] += my_header + "\n"
                    resp['msg'] += '-' * len(my_header) + "\n"
                    resp['msg'] += my_fmt.format(**job_stats) + "\n"
    else:
        resp['msg'] += "No jobs found matching search criteria" + "\n"

    return json.dumps(resp)

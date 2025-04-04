# qhist

## Details
The PBS Professional scheduler includes the `qstat` command for querying active
and (optionally) recently completed jobs. Users *may* be given access to the
`tracejob` command as well, which can interrogate the historical records of a
specified job. However, it does not come with a tool to query all historical job
records.

This package aims to provide such a tool in the form of `qhist`. Modeled after
the `sacct` command in the Slurm scheduler, `qhist` allows the user to display
PBS accounting records in tabular, csv, json, and long forms.

## Installation

`qhist` was written to require minimal dependencies. As such, only the following
are required:

* Python >= 3.6
* [pbsparse](https://github.com/NCAR/pbsparse)

There are two ways to install `qhist`, with `pip` or using the included `Makefile`.

### Installing with `pip`

`qhist` is provided in package form as `pbs-qhist` via PyPI. To install `qhist`
this way, simply run the following:

```shell
python3 -m pip install pbs-qhist
```

### Installing with `make`

You may wish to provide qhist as a standalone application with its own `bin` and
`man` directories. This can be done with the provided Makefile.

```shell
make install [PREFIX=/home/$USER/qhist]
```

### Configuration

`qhist` will attempt to search for PBS accounting records at the default path:

```
$PBS_HOME/server_priv/accounting
```

However, this path is only written to on the server and thus has limited
utility. Typically, site administrators will want to mount/rsync/etc these files
to a path on hosts accessible to users (e.g., front-end nodes). In this
scenario, `qhist` will need some help in determining where to find the logs.

Additional configuration can be specified in three ways, ordered in descending
precedence:

1. Set the environment variable `QHIST_SERVER_CONFIG` to the path of your
   configuration file.
2. Put your configuration into `server.json` within the `cfg` subdirectory of
   your `qhist` installation.
3. Create a configuration file at `/etc/qhist/server.json`.

All configuration in `default.json` can be overridden in your server
configuration file as well.

## Usage

If run with no options, `qhist` will display the "end" record data for all jobs
that have finished on the current calendar day.

```
positional arguments:
  jobid                 one or more job IDs

optional arguments:
  -h, --help            show this help message and exit
  -A ACCOUNT, --account ACCOUNT
                        filter jobs by a specific account/project code
  -a, --average         print average resource statistics in default/wide mode
  -c, --csv             output jobs in csv format
  -d DAYS, --days DAYS  number of days prior to search (default = 0)
  -e EVENTS, --events EVENTS
                        list of events to display (E=end, R=requeue)
  -F FILTER, --filter FILTER
                        specify a freeform filter (--filter=help for more)
  -f FORMAT, --format FORMAT
                        use custom format (--format=help for more)
  -H HOSTS, --hosts HOSTS
                        only print jobs that ran on specified comma-delimited list of nodes
  -j, --json            output jobs in json format
  -l, --list            display untruncated output in list format
  -N JOBNAME, --name JOBNAME
                        only print jobs that have the specified job name
  -n, --nodes           show list of nodes for each job
  --noheader            do not display a header for tabular output
  -p PERIOD, --period PERIOD
                        search over specific date range (YYYYMMDD-YYYYMMDD or YYYYMMDD for a single day)
  -q QUEUE, --queue QUEUE
                        filter jobs by a specific queue
  -r, --reverse         print jobs in reverse order
  -s EXIT_STATUS, --status EXIT_STATUS
                        only print jobs with specified exit status
  -t {s,m,h,d}, --time {s,m,h,d}
                        display time deltas in seconds, minutes, or hours (default)
  -u USER, --user USER  filter jobs by a specific user
  -W WAIT, --wait WAIT  show jobs with queue waits above value (mins)
  -w, --wide            use wide table columns and show job names
```

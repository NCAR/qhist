import datetime, re, sys

NODE_REGEX = re.compile('\(([^:]*)')

class PbsRecord:
    def __init__(self, record_data, process = False):
        time_stamp, record_type, job_id, record_meta = record_data.split(";")

        self.time = datetime.datetime.strptime(time_stamp, "%m/%d/%Y %H:%M:%S")
        self.type = record_type
        self.id = job_id
        self.fill_value = None

        for name, value in (item.split("=", 1) for item in record_meta.split()):
            if "-" in name:
                name = name.replace("-", "_")

            if "." in name:
                category, name = name.split(".")

                try:
                    getattr(self, category)[name] = value
                except AttributeError:
                    setattr(self, category, { name : value })
            else:
                setattr(self, name, value)

        if process:
            self.process_record()

    def __str__(self):
        return f"{self.type} record at {self.time} for job {self.id}"

    def process_record(self):
        try:
            self.account = self.account.replace('"', "")
        except AttributeError:
            pass

        try:
            self.run_count = int(self.run_count)
        except (AttributeError, ValueError) as e:
            pass

        try:
            self.Resource_List["mem"] = float(self.Resource_List["mem"][:-2]) * mem_factor(self.Resource_List["mem"][-2:])
        except KeyError:
            pass
        except (ValueError, TypeError) as e:
            self.Resource_List["mem"] = 0

        try:
            self.Resource_List["walltime"] = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.Resource_List["walltime"].split(':'))) / 3600.0
        except (KeyError, ValueError) as e:
            pass
        
        try:
            self.eligible_time = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.eligible_time.split(':'))) / 3600.0
        except (AttributeError, ValueError) as e:
            pass

        try:
            self.waittime = (int(self.start) - int(self.etime)) / 3600.0
        except AttributeError:
            pass

        for time_var in ("ctime", "etime", "start", "end"):
            try:
                raw_value = getattr(self, time_var)
                setattr(self, time_var, datetime.datetime.fromtimestamp(float(raw_value)))
            except (AttributeError, ValueError) as e:
                pass

        for list_var in ("ncpus", "ngpus", "nodect"):
            try:
                self.Resource_List[list_var] = int(self.Resource_List[list_var])
            except (KeyError, ValueError) as e:
                pass

        if "[]" not in self.id:
            if hasattr(self, "resources_used"):
                for mem_type in ("mem", "vmem"):
                    try:
                        self.resources_used[mem_type] = float(self.resources_used[mem_type][:-2]) * mem_factor(self.resources_used[mem_type][-2:])
                    except KeyError:
                        pass
                    except ValueError:
                        self.resources_used[mem_type] = 0
                
                for time_var in ("walltime", "cput"):
                    try:
                        self.resources_used[time_var] = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.resources_used[time_var].split(':'))) / 3600.0
                    except (KeyError, ValueError) as e:
                        pass

                for list_var in ("cpupercent", "ncpus"):
                    try:
                        self.resources_used[list_var] = int(self.resources_used[list_var])

                        if list_var == "cpupercent":
                            self.resources_used["avgcpu"] = float(self.resources_used["cpupercent"]) / self.Resource_List["ncpus"]
                    except (KeyError, ValueError, ZeroDivisionError) as e:
                        pass
            elif hasattr(self, "resource_assigned"):
                for mem_type in ("mem", "vmem"):
                    try:
                        self.resource_assigned[mem_type] = float(self.resource_assigned[mem_type][:-2]) * mem_factor(self.resource_assigned[mem_type][-2:])
                    except KeyError:
                        pass
                    except ValueError:
                        self.resources_used[mem_type] = 0
                
                try:
                    self.resource_assigned["ncpus"] = int(self.resource_assigned["ncpus"])
                except (KeyError, ValueError) as e:
                    pass

    def get_chunks(self):
        chunks = []

        for chunk_spec in self.Resource_List["select"].split("+"):
            chunk = {}
            chunk["count"], *resources = chunk_spec.split(":")

            for res_spec in resources:
                name, value = res_spec.split("=", 1)

                if name == "mem":
                    chunk[name] = float(value[:-2]) * mem_factor(value[-2:])
                else:
                    try:
                        chunk[name] = int(value)
                    except ValueError:
                        chunk[name] = value

            chunks.append(chunk)

        return chunks

    def get_nodes(self):
        try:
            return NODE_REGEX.findall(self.exec_vnode)
        except AttributeError:
            return "-"

def mem_factor(units):
    if units == "gb":
        return 1
    elif units == "tb":
        return 1024
    elif units == "mb":
        return (1.0 / 1024)
    elif units == "kb":
        return (1.0 / 1048576)

def get_pbs_records(data_file, process = False, record_filter = None, data_filters = {}):
    with open(data_file, "r") as paf:
        for record in paf:
            if not record_filter or record[20] in record_filter:
                match = True
                event = PbsRecord(record, process)
                
                for criteria in data_filters:
                    if isinstance(data_filters[criteria], str):
                        if getattr(event, criteria) != data_filters[criteria]:
                            match = False
                            break
                    elif isinstance(data_filters[criteria], list):
                        if "joblist" in criteria:
                            if not any(event.id.startswith(job_id) for job_id in data_filters["joblist"]):
                                match = False
                                break
                        elif "hosts" in criteria:
                            job_nodes = event.get_nodes()

                            if not all(mom in job_nodes for mom in data_filters["hosts"]):
                                match = False
                                break
                
                if match:
                    yield event

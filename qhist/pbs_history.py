import datetime, re, sys, os, io

from typing import Optional, BinaryIO

NODE_REGEX = re.compile('\(([^:]*)')

class PbsRecord:
    def __init__(self, record_data, process = False, time_divisor = 1.0):
        time_stamp, record_type, job_id, record_meta = record_data.split(";")

        self.time = datetime.datetime.strptime(time_stamp, "%m/%d/%Y %H:%M:%S")
        self.type = record_type
        self.id = job_id
        self.short_id = self.id.split(".")[0]
        self._divisor = time_divisor

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
            self.Resource_List["walltime"] = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.Resource_List["walltime"].split(':'))) / self._divisor
        except (KeyError, ValueError) as e:
            pass

        try:
            self.eligible_time = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.eligible_time.split(':'))) / self._divisor
        except (AttributeError, ValueError) as e:
            pass

        try:
            self.waittime = (int(self.start) - int(self.etime)) / self._divisor
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
                        self.resources_used[time_var] = sum(int(x) * 60 ** (2 - i) for i, x in enumerate(self.resources_used[time_var].split(':'))) / self._divisor
                    except (KeyError, ValueError) as e:
                        pass

                for list_var in ("ncpus", "cpupercent"):
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

# Nice solution from https://stackoverflow.com/a/78770925
class ReverseOpen:
    def __init__(self, file_path: str, *, encoding: Optional[str]='utf-8', buffer_size: Optional[int]=None):
        self.file_path = file_path
        self.encoding = encoding
        self.buffer_size = io.DEFAULT_BUFFER_SIZE if (buffer_size is None) else buffer_size
        self._file: Optional[BinaryIO] = None
        self._file_size: Optional[int] = None

    def __iter__(self):
        for line in self.lines():
            yield self.decode(line)

    def __enter__(self):
        self.open()
        return self
  
    def __exit__(self, *args, **kwargs):
        self.close()
  
    def open(self):
        if self._file is None:
            self._file = open(self.file_path, 'rb')
            
            try:
                self._file_size = self._file.seek(0, os.SEEK_END)
            except:
                self.close()
                raise

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file_size = None

    def decode(self, _bytes: bytes):
        if self.encoding is None:
            return _bytes
        else:
            return _bytes.decode(self.encoding)

    def lines(self):
        # reverse iterate lines, except for last line if empty
        iter_lines = self._lines()
        last_line = next(iter_lines)
        
        if last_line:
            yield last_line
        
        yield from iter_lines

    def _lines(self):
        # reverse iterate lines from stitching file chunks
        def iter_cur_bytes():
            yield chunk[left_chunk_i : right_chunk_i]
            yield from reversed(line)

        line: list[bytes] = []

        for chunk in self.chunks():
            right_chunk_i = len(chunk)
          
            for left_chunk_i in self.line_starts(chunk):
                yield b''.join(iter_cur_bytes())
                del line[:]
                right_chunk_i = left_chunk_i
          
            if right_chunk_i:
                line.append(chunk[:right_chunk_i])

        yield b''.join(reversed(line))

    def chunks(self):
        # reverse iterate chunks from file offsets
        right_i = self._file_size

        for offset in self.chunk_offsets():
            self._file.seek(offset)
            yield self._file.read(right_i - offset)
            right_i = offset

    def chunk_offsets(self):
        # reverse iterate file.seek() offsets for seeking to beginning of chunks
        yield from range(self._file_size - self.buffer_size, 0, -self.buffer_size)
        yield 0

    @staticmethod
    def line_starts(chunk: bytes):
        # reverse iterate byte indexes that are the start of a line
        for byte_i in range(len(chunk) - 1, -1, -1):
            if ord('\n') == chunk[byte_i]:
                yield byte_i + 1

def mem_factor(units):
    if units == "gb":
        return 1
    elif units == "tb":
        return 1024
    elif units == "mb":
        return (1.0 / 1024)
    elif units == "kb":
        return (1.0 / 1048576)

def get_pbs_records(data_file, process = False, type_filter = None,
                    id_filter = None, host_filter = None, data_filters = None,
                    reverse = False, time_divisor = 1.0):
    if reverse:
        cm = ReverseOpen(data_file)
    else:
        cm = open(data_file, "r")

    with cm as records:
        for record in records:
            if not type_filter or record[20] in type_filter:
                match = True
                event = PbsRecord(record, process, time_divisor = time_divisor)

                if not id_filter or any(event.short_id.startswith(job) for job in id_filter):
                    if host_filter:
                        job_nodes = event.get_nodes()

                        if not all(mom in job_nodes for mom in host_filter):
                            continue

                    for negation, operation, field, expected in data_filters:
                        if "[" in field:
                            field_dict, field_key = field.split("[")
                            value = getattr(event, field_dict)[field_key[:-1]]
                        else:
                            value = getattr(event, field)

                        if operation(value, type(value)(expected)) ^ (not negation):
                            match = False
                            break

                    if match:
                        yield event

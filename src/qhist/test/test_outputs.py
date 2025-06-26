import pytest
from qhist import qhist
from pbsparse import PbsRecord

data = '03/31/2025 11:39:29;E;4215065.casper-pbs;user=vanderwb group=csgteam account="SCSG0001" project=_pbs_project_default jobname=STDIN queue=htc ctime=1743440746 qtime=1743440746 etime=1743440746 start=1743440753 exec_host=crhtc82/32 exec_vnode=(crhtc82:ncpus=1:mem=31457280kb) Resource_List.mem=30gb Resource_List.mps=0 Resource_List.ncpus=1 Resource_List.ngpus=0 Resource_List.nodect=1 Resource_List.nvpus=0 Resource_List.place=scatter Resource_List.select=1:ncpus=1:mem=30GB:ompthreads=1 Resource_List.walltime=06:00:00 session=97162 end=1743442769 Exit_status=0 resources_used.cpupercent=34 resources_used.cput=00:00:16 resources_used.mem=753360kb resources_used.ncpus=1 resources_used.vmem=8722444kb resources_used.walltime=00:33:27 eligible_time=00:00:15 run_count=1'
fields = ("user","resources_used[mem]","etime")

def test_tabular_default():
    record = PbsRecord(data, process = True)
    output = qhist.tabular_output(vars(record), "{id:10.10} {Resource_List[ncpus]:>6d} {etime:%m%dT%H%M}")
    assert output == "4215065.ca      1 0331T1105"

def test_long_output(capsys):
    record = PbsRecord(data, process = True)
    qhist.list_output(record, fields,{"user" : "User", "resources_used[mem]" : "Req Mem (GB)", "etime" : "End Time" }, "{:20} = {}")
    captured = capsys.readouterr()
    expected = """4215065.casper-pbs
User                 = vanderwb
Req Mem (GB)         = 0.72
End Time             = 2025-03-31 11:05:46

"""

    assert captured.out == expected

def test_csv_output(capsys):
    record = PbsRecord(data, process = True)
    qhist.csv_output(record, fields)
    captured = capsys.readouterr()
    assert captured.out == "vanderwb,0.7184600830078125,2025-03-31 11:05:46\n"

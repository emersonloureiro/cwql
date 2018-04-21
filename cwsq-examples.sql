
CLI
	aws cloudwatch get-metric-statistics --metric-name CPUUtilization --start-time 2018-03-10T00:00:00Z --end-time 2018-03-10T14:00:00Z --period 3600 --namespace AWS/EC2 --statistics Maximum Average --dimensions Name=InstanceId,Value=i-00c753b8c2e2273a9

CWQL
	correct
		SELECT max(CPUUtilization), avg(CPUUtilization) FROM AWS/EC2 WHERE InstanceId='i-00c753b8c2e2273a9' BETWEEN 2018-03-10T00:00:00Z AND 2018-03-10T14:00:00Z PERIOD 3600
		SELECT max(ec21.CPUUtilization), avg(ec22.CPUUtilization) FROM AWS/EC2 as ec21, AWS/EC2 as ec22  WHERE ec21.InstanceId='i-00c753b8c2e2273a9' AND ec22.InstanceId='i-00c753b8c2e2273a9' BETWEEN 2018-03-10T00:00:00Z AND 2018-03-10T14:00:00Z PERIOD 3600
		select avg(CPUUtilization) from AWS/EC2 where InstanceId='i-00c753b8c2e2273a9' between 2018-03-10T00:00:00Z and 2018-03-10T14:00:00Z period 3660
		select avg(ec2.CPUUtilization) from AWS/EC2 as ec2 where InstanceId='i-00c753b8c2e2273a9' between 2018-03-10T13:00:00Z and 2018-03-10T14:00:00Z period 60

	invalid
		SELECT max(ec2.CPUUtilization), avg(ec2.CPUUtilization) FROM AWS/EC2 as ec2  WHERE InstanceId='i-00c753b8c2e2273a9' BETWEEN 2018-03-10T00:00:00Z AND 2018-03-10T14:00:00Z PERIOD 3600
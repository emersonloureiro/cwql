aws cloudwatch get-metric-statistics --metric-name CPUUtilization --start-time 2018-03-10T13:00:00Z --end-time 2018-03-10T14:00:00Z --period 60 --namespace AWS/EC2 --statistics Maximum Average --dimensions Name=InstanceId,Value=i-00c753b8c2e2273a9

aws --endpoint https://monitoring.amazonaws.com cloudwatch get-metric-statistics --metric-name CPUUtilization --start-time 2018-03-10T13:00:00Z --end-time 2018-03-10T14:00:00Z --period 60 --namespace AWS/EC2 --statistics Maximum Average --dimensions Name=InstanceId,Value=i-00c753b8c2e2273a9

aws cloudwatch get-metric-statistics --metric-name CPUUtilization --start-time 2018-03-10T13:00:00Z --end-time 2018-03-10T14:00:00Z --period 3600 --namespace AWS/EC2 --statistics Maximum Average --dimensions Name=InstanceId,Value=i-00c753b8c2e2273a9

select avg(CPUUtilization) from AWS/EC2 where InstanceId='i-00c753b8c2e2273a9' between 2018-03-10T00:00:00Z and 2018-03-10T14:00:00Z period 60

select avg(ec2.CPUUtilization) from AWS/EC2 as ec2 where InstanceId='i-00c753b8c2e2273a9' between 2018-03-10T13:00:00Z and 2018-03-10T14:00:00Z period 60
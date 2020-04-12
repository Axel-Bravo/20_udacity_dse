import os
from .aws_library import AwsParameters, AwsExecutor

print(os.getcwd())
test = AwsParameters(configuration_path='dwh.cfg', configuration_path_aws='dwh_user.cfg')

test_2 = AwsExecutor(aws_config=test)
test_2.create_redshift()
test_2.redshift_status()
print('stopper')

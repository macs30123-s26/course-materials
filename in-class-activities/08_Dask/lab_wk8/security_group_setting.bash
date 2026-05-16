# Find default VPC
VPC_ID=$(aws ec2 describe-vpcs \
  --filters Name=isDefault,Values=true \
  --query "Vpcs[0].VpcId" \
  --output text)

# Find one subnet in that VPC
SUBNET_ID=$(aws ec2 describe-subnets \
  --filters Name=vpc-id,Values=$VPC_ID \
  --query "Subnets[0].SubnetId" \
  --output text)

# Create security group
SG_ID=$(aws ec2 create-security-group \
  --group-name dask-ec2-sg \
  --description "Security group for Dask EC2Cluster" \
  --vpc-id $VPC_ID \
  --query "GroupId" \
  --output text)

# Your current public IP
MY_IP=$(curl -s https://checkip.amazonaws.com)/32

# Allow scheduler and dashboard from your machine
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 8786 \
  --cidr $MY_IP

aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 8787 \
  --cidr $MY_IP

# Optional: SSH
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 22 \
  --cidr $MY_IP

# Allow cluster nodes to talk to each other
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol -1 \
  --source-group $SG_ID

echo $VPC_ID
echo $SUBNET_ID
echo $SG_ID
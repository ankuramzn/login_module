namespace :emr do

  # rake emr:connect
  desc "Establish EMR connection"
  task :connect do
    puts "Establishing connection with the EMR Service"
    @emr = Elasticity::EMR.new("AKIAJMMK7EVCSVI4UIYQ", "aBXAEV9dDvqytgs7BF6aEXrnI2tYuxSKAEhpgSTS")
  end

  # rake emr:list_all_jobs
  desc "List all EMR Jobs in all Status"
  task :list_all_jobs => :connect do
    puts "Listing all Job FLows in all Statuses"
    jobflows_by_status = @emr.describe_jobflows.group_by { |jobflow| jobflow.state }
    jobflows_by_status.each {|status, jobflows|
      print "#{status} -- "
      p jobflows.map(&:jobflow_id)
    }
  end

  # rake "emr:list_all_jobs_in_status[WAITING]"
  desc "List all EMR Jobs by status WAITING (default), TERMINATED, COMPLETED, FAILED"
  task :list_all_jobs_in_status, [:job_status] => [:connect] do |t, args|
    args.with_defaults(:job_status => "WAITING")
    puts "Listing all Running Jobs for the account in status " + args.job_status
    jobflows = @emr.describe_jobflows.find_all { |jobflow| jobflow.state == args.job_status }
    p jobflows.map(&:jobflow_id)
  end


  # rake "emr:list_all_jobs_in_status_2[WAITING]"
  desc "2. List all EMR Jobs by status WAITING (default), TERMINATED, COMPLETED, FAILED"
  task :list_all_jobs_in_status_2, [:job_status] do |t, args|
    args.with_defaults(:job_status => "WAITING")
    puts "Listing all Running Jobs for the account in status " + args.job_status
    @aws_request = Elasticity::AwsRequest.new("AKIAJMMK7EVCSVI4UIYQ", "aBXAEV9dDvqytgs7BF6aEXrnI2tYuxSKAEhpgSTS")
    params = {
         :operation => "DescribeJobFlows",
         :JobFlowStates => [args.job_status]
       }
    aws_result = @aws_request.aws_emr_request(Elasticity::EMR.convert_ruby_to_aws(params))
    xml_doc = Nokogiri::XML(aws_result)
    xml_doc.remove_namespaces!
    yield aws_result if block_given?
    jobflows = Elasticity::JobFlow.from_members_nodeset(xml_doc.xpath("/DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member"))
    p jobflows.map(&:jobflow_id)
  end


  # rake "emr:emr_job__status[j-U34KGNIRVN6J]"
  desc "Check specific EMR Job Status"
  task :emr_job__status, [:jobId]  do |t, args|
    puts "Status Check for EMR job " + args.jobId
    @aws_request = Elasticity::AwsRequest.new("AKIAJMMK7EVCSVI4UIYQ", "aBXAEV9dDvqytgs7BF6aEXrnI2tYuxSKAEhpgSTS")
    params = {
         :operation => "DescribeJobFlows",
         :JobFlowIds => [args.jobId]
       }
    aws_result = @aws_request.aws_emr_request(Elasticity::EMR.convert_ruby_to_aws(params))

    puts aws_result.inspect

    xml_doc = Nokogiri::XML(aws_result)
    xml_doc.remove_namespaces!

    puts xml_doc.inspect
    yield aws_result if block_given?
    jobflows = Elasticity::JobFlow.from_members_nodeset(xml_doc.xpath("/DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member"))
    puts jobflows.first.inspect
    puts "Id " + jobflows.first.jobflow_id + " Name " + jobflows.first.name + " State " + jobflows.first.state
  end

  # rake "emr:terminate_emr_job[jobId]"
  desc "Terminate specific EMR Job"
  task :terminate_emr_job, [:jobId] => [:connect] do |t, args|
    puts "Terminating " + args.jobId
     @emr.terminate_jobflows(args.jobId)
  end


 #$ ./elastic-mapreduce --create --stream \
 #    --mapper  s3://elasticmapreduce/samples/wordcount/wordSplitter.py \
 #    --input   s3://elasticmapreduce/samples/wordcount/input \
 #    --output  [A path to a bucket you own on Amazon S3, such as, s3n://my-bucket] \
 #    --reducer aggregate

#         "Name": "streaming job flow",
#"HadoopJarStep":
#	{
#         "Jar": "/home/hadoop/contrib/streaming/hadoop-streaming.jar",
#        "Args":
#           [
#            "-input",   "s3n://elasticmapreduce/samples/wordcount/input",
#            "-output",  "s3n://YourBucket",
#            "-mapper",  "s3://elasticmapreduce/samples/wordcount/wordSplitter.py",
#            "-reducer", "aggregate"
#           ]
#    }

  desc "Start the most basic Streaming EMR Job"
  task :initiate_basic_job => [:connect, :environment]  do


    s3_conn = AmazonS3Asset.new
    s3_conn.create_bucket("ankur-suki")

    jobflow_id = @emr.run_job_flow(
        {
            :name => "Basic Streaming Job",
            :instances => {
                :ec2_key_name => "09_01_2011",
                :hadoop_version => "0.20",
                :instance_count => 2,
                :master_instance_type => "m1.small",
                :placement => {
                    :availability_zone => "us-east-1a"
                },
                :slave_instance_type => "m1.small",
                :keep_job_flow_alive_when_no_steps => false,
            },
            :steps =>
                [
                    {
                        :action_on_failure => "TERMINATE_JOB_FLOW",
                        :hadoop_jar_step =>
                            {
                                :args =>
                                    [
                                      "-input",   "s3n://elasticmapreduce/samples/wordcount/input",
                                      "-output",  "s3n://ankur-suki",
                                      "-mapper",  "s3://elasticmapreduce/samples/wordcount/wordSplitter.py",
                                      "-reducer", "aggregate"
                                    ],
                                :jar => "/home/hadoop/contrib/streaming/hadoop-streaming.jar"
                            },
                        :name => "Word Counting Step"
                    }
                    #{
                    #    :action_on_failure => "TERMINATE_JOB_FLOW",
                    #    :hadoop_jar_step =>
                    #        {
                    #            :args =>
                    #                [
                    #                    "s3://elasticmapreduce/libs/pig/pig-script",
                    #                    "--run-pig-script",
                    #                    "--args",
                    #                    "-p",
                    #                    "INPUT=s3n://elasticmapreduce/samples/pig-apache/input",
                    #                    "-p",
                    #                    "OUTPUT=s3n://slif-elasticity/pig-apache/output/2011-04-19",
                    #                    "s3n://elasticmapreduce/samples/pig-apache/do-reports.pig"
                    #                ],
                    #            :jar => "s3://elasticmapreduce/libs/script-runner/script-runner.jar"
                    #        },
                    #    :name => "Run Pig Script"
                    #}
                ]
        }
    )
    puts jobflow_id

  end

  desc "Start Streaming EMR Job"
  task :check do
    puts "Hi"
    emr = Elasticity::EMR.new("AKIAJMMK7EVCSVI4UIYQ", "aBXAEV9dDvqytgs7BF6aEXrnI2tYuxSKAEhpgSTS")

    puts emr.inspect
    jobflow_id = emr.run_job_flow(
        {
            :name => "Elasticity Test Flow (EMR Pig Script)",
            :instances => {
                :ec2_key_name => "09_01_2011",
                :hadoop_version => "0.20",
                :instance_count => 2,
                :master_instance_type => "m1.small",
                :placement => {
                    :availability_zone => "us-east-1a"
                },
                :slave_instance_type => "m1.small",
                :keep_job_flow_alive_when_no_steps => true,
            },
            :steps =>
                [
                    #{
                    #    :action_on_failure => "TERMINATE_JOB_FLOW",
                    #    :hadoop_jar_step =>
                    #        {
                    #            :args =>
                    #                [
                    #                    "s3://elasticmapreduce/libs/pig/pig-script",
                    #                    "--base-path",
                    #                    "s3://elasticmapreduce/libs/pig/",
                    #                    "--install-pig"
                    #                ],
                    #            :jar => "s3://elasticmapreduce/libs/script-runner/script-runner.jar"
                    #        },
                    #    :name => "Setup Pig"
                    #},
                    #{
                    #    :action_on_failure => "TERMINATE_JOB_FLOW",
                    #    :hadoop_jar_step =>
                    #        {
                    #            :args =>
                    #                [
                    #                    "s3://elasticmapreduce/libs/pig/pig-script",
                    #                    "--run-pig-script",
                    #                    "--args",
                    #                    "-p",
                    #                    "INPUT=s3n://elasticmapreduce/samples/pig-apache/input",
                    #                    "-p",
                    #                    "OUTPUT=s3n://slif-elasticity/pig-apache/output/2011-04-19",
                    #                    "s3n://elasticmapreduce/samples/pig-apache/do-reports.pig"
                    #                ],
                    #            :jar => "s3://elasticmapreduce/libs/script-runner/script-runner.jar"
                    #        },
                    #    :name => "Run Pig Script"
                    #}
                ]
        }
    )
    puts jobflow_id
  end

end

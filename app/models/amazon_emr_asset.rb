class AmazonEmrAsset

  # Establish connection with the AWS EMR service
  def initialize
    puts "connecting..."
    @@s3_config_path ||= RAILS_ROOT + '/config/amazon_s3.yml'
    @@s3_config ||=
         YAML.load_file(@@s3_config_path)[Rails.env].symbolize_keys
    @emr = Elasticity::EMR.new(
        @@s3_config[:access_key_id],
        @@s3_config[:secret_access_key]
    )
    @aws_request = Elasticity::AwsRequest.new(
        @@s3_config[:access_key_id],
        @@s3_config[:secret_access_key]
    )
  end

  # Universal Job Flow Statuses - COMPLETED | FAILED | TERMINATED | RUNNING | SHUTTING_DOWN | STARTING | WAITING | BOOTSTRAPPING
  def list_all_emr_jobs(job_status=nil)
    if job_status.nil?
      puts "Listing all Job FLows in all Statuses"
      jobflows_by_status = @emr.describe_jobflows.group_by { |jobflow| jobflow.state }
      #jobflows_by_status.each {|status, jobflows|
      #  print "In #{status} Jobs are -- "
      #  p jobflows.map(&:jobflow_id)
      #}
    else
      puts "Listing all Jobs for in status " + job_status

      params = {
           :operation => "DescribeJobFlows",
           :JobFlowStates => [job_status]
         }
      aws_result = @aws_request.aws_emr_request(Elasticity::EMR.convert_ruby_to_aws(params))
      xml_doc = Nokogiri::XML(aws_result)
      xml_doc.remove_namespaces!
      yield aws_result if block_given?
      jobflows_by_status = Elasticity::JobFlow.from_members_nodeset(xml_doc.xpath("/DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member"))
      #p jobflows_by_status.map(&:jobflow_id)
    end
    jobflows_by_status
  end

  # Details for a specific Job Flow
  def job_details(jobId)
    puts "Status Check for EMR job " + jobId

    params = {
         :operation => "DescribeJobFlows",
         :JobFlowIds => [jobId]
       }
    aws_result = @aws_request.aws_emr_request(Elasticity::EMR.convert_ruby_to_aws(params))

    xml_doc = Nokogiri::XML(aws_result)
    xml_doc.remove_namespaces!

    yield aws_result if block_given?
    jobflows = Elasticity::JobFlow.from_members_nodeset(xml_doc.xpath("/DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member"))
    #puts jobflows.first.inspect
    jobflows
  end

  # Terminate a specific Job flow
  def terminate(jobId)
    puts "Terminating EMR Job " + jobId
    @emr.terminate_jobflows(jobId)
  end

  # Start an empty Streaming Job
  def start_streaming_emr_jobflow
    jobflow_id = @emr.run_job_flow(
        {
            :name => "Empty Streaming Job " + DateTime.now.to_s,
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
            :steps => []
        }
    )
    jobflow_id
  end

  # Add Steps to a WAITING Job Flow
  def add_steps(
          jobflow_id,
          name="Word Counting Step",
          input="s3n://elasticmapreduce/samples/wordcount/input",
          mapper="s3://elasticmapreduce/samples/wordcount/wordSplitter.py",
          reducer="aggregate" )

    @emr.add_jobflow_steps(
        jobflow_id,
        {
          :steps => [
              {
                  :action_on_failure => "TERMINATE_JOB_FLOW",
                  :name => name,
                  :hadoop_jar_step => {
                      :args => [
                          "-input",   input,
                          "-output",  's3n://' + Time.now.to_i.to_s + '/',
                          "-mapper",  mapper,
                          "-reducer", reducer
                      ],
                      :jar => "/home/hadoop/contrib/streaming/hadoop-streaming.jar"
                  }
              }
          ]
        }
    )

  end
end


require 'aws/s3'

class AmazonS3Asset

  include AWS::S3

  def initialize
    puts "connecting..."
    @@s3_config_path ||= RAILS_ROOT + '/config/amazon_s3.yml'
    @@s3_config ||=
         YAML.load_file(@@s3_config_path)[Rails.env].symbolize_keys
    AWS::S3::Base.establish_connection!(
      :access_key_id     => @@s3_config[:access_key_id],
      :secret_access_key => @@s3_config[:secret_access_key]
      )
  end

  #==== BUCKET OPERATIONS ======#
  # List all the buckets for the S3 account
  def list_buckets
    Service.buckets
  end

  # Create a new S3 bucket using name
  def create_bucket(name)
    Bucket.create(name)
  end

  def delete_bucket(name)
    Bucket.delete(name)
  end


  # Get keys for all S3Objects in a given bucket
  def bucket_keys(bucket)
    b = Bucket.find(bucket)
    b.objects.collect {|o| o.key}
  end

  # Get all S3Objects in a given bucket
  def bucket_objects(bucket)
    Bucket.objects(bucket)
  end


  #==== OBJECT OPERATIONS ======#
  # Delete a S3Object from a bucket using its key
  def delete_key(bucket, key)
    if exists?(bucket, key)
      S3Object.delete key, bucket
    end
  end

  # Delete all S3Objects from a bucket
  def empty_bucket(bucket)
    bucket_keys(bucket).each do |k|
      puts "deleting #{k}"
      delete_key(bucket,k)
    end
  end


  # Get one S3Object in a given bucket with a given key
  def bucket_object(bucket, key)
    S3Object.find key, bucket
  end

  def copy_over_bucket(from_bucket, to_bucket)
    puts "Replacing #{to_bucket} with contents of #{from_bucket}"
    #delete to_bucket
    empty_bucket(to_bucket)
    bucket_keys(from_bucket).each do |k|
      copy_between_buckets(from_bucket, to_bucket, k)
    end
  end

  def copy_between_buckets(from_bucket, to_bucket, from_key, to_key = nil)
    if exists?(from_bucket, from_key)
      to_key = from_key if to_key.nil?
      puts "Copying #{from_bucket}.#{from_key} to #{to_bucket}.#{to_key}"
      url = "http://s3.amazonaws.com/#{from_bucket}/#{from_key}"
      filename = download(url)
      store_file(to_bucket,to_key,filename)
      File.delete(filename)
      return "http://s3.amazonaws.com/#{to_bucket}/#{to_key}"
    else
      puts "#{from_bucket}.#{from_key} didn't exist"
      return nil
    end
  end

  # Store binary object in the given bucket, with the given key
  def store_object(bucket, key, object)
     puts "Storing object in #{bucket}.#{key}"
     S3Object.store(
      key,
      object,
      bucket,
      :access => :public_read
      )
  end

  # Store a local file in the given bucket, with the given key
  def store_file(bucket, key, filename)
     puts "Storing #{filename} in #{bucket}.#{key}"
     S3Object.store(
      key,
      File.open(filename),
      bucket,
      :access => :public_read
      )
  end

  #def download(url, save_as = nil)
  #  if save_as.nil?
  #    Dir.mkdir("amazon_s3_temp") if !File.exists?("amazon_s3_temp")
  #    save_as = File.join("amazon_s3_temp",File.basename(url))
  #  end
  #  begin
  #    puts "Saving #{url} to #{save_as}"
  #    agent = WWW::Mechanize.new {|a| a.log = Logger.new(STDERR) }
  #    img = agent.get(url)
  #    img.save_as(save_as)
  #    return save_as
  #  rescue
  #    raise "Failed on " + url + "  " + save_as
  #  end
  #end

  def exists?(bucket,key)
    begin
      res = S3Object.find key, bucket
    rescue
      res = nil
    end
    return !res.nil?
  end

end

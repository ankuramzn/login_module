class UsersController < ApplicationController
  def new
    @user = User.new
  end

  def create
    @user = User.new(params[:user])
    if @user.save
      #redirect_to :controller => "users", :action => "new", :notice => "User created"
      redirect_to root_url, :notice => "User created"
    else
      render "new"
    end
  end

end

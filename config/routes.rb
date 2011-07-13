Auth4::Application.routes.draw do
  get "sessions/new"

  get "sign_up" => "users#new", :as => "sign_up"
  root :to => "users#new"
  resources :users


  get "log_in" => "sessions#new", :as => "log_in"
  get "log_out" => "sessions#destroy", :as => "log_out"

  resources :sessions


end

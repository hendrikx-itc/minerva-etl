# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure "2" do |config|
  config.vm.box = "puppetlabs/ubuntu-14.04-64-nocm"
  config.vm.hostname = "vagrant-minerva"

  config.vm.provider 'virtualbox' do |v|
    config.vm.network :private_network, ip: '10.11.12.13'
    config.vm.network "forwarded_port", guest: 5432, host: 5431, protocol: 'tcp'
  end

  config.vm.provision :salt do |salt|
    salt.minion_config = "provision/salt/minion"
    salt.run_highstate = true
    salt.verbose = true
  end
end

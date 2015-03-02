/home/vagrant/bin:
  file:
    - recurse
    - source: salt://resources/commands
    - user: vagrant
    - group: vagrant
    - file_mode: 755
    - makedirs: True

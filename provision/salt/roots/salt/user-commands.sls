/home/vagrant/bin/rebuild-database:
  file:
    - managed
    - source: salt://resources/rebuild-database
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True

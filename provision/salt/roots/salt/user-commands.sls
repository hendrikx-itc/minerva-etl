/home/vagrant/bin/rebuild-database:
  file:
    - managed
    - source: salt://resources/rebuild-database
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True

/home/vagrant/bin/create-database:
  file:
    - managed
    - source: salt://resources/create-database
    - user: vagrant
    - group: vagrant
    - mode: 755
    - makedirs: True

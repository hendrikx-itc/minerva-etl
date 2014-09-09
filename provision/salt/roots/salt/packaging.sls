packaging-prerequisites:
  pkg:
    - names:
      - debhelper
      - python-sphinx
    - installed

/home/vagrant/packaging/src:
  mount.mounted:
    - device: /vagrant
    - fstype: bind
    - opts: bind
    - mkmnt: True
    - mount: True

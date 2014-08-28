base-packages:
  pkg:
    - installed
    - names:
      - vim
      - zsh
      - tmux

set-timezone:
  cmd.run:
  - name: 'echo "Europe/Amsterdam" > /etc/timezone && dpkg-reconfigure -f noninteractive tzdata && touch /tmp/timezone-is-set'
  - creates: /tmp/timezone-is-set

vagrant-unix-user:
  user:
    - present
    - name: vagrant
    - shell: /bin/zsh

/home/vagrant/.zshrc:
  file.managed:
    - source: salt://resources/.zshrc
    - user: vagrant
    - group: vagrant
    - mode: 644

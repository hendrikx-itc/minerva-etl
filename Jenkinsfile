pipeline {
  stages {
    stage('unit tests') {
      steps {
        script {
            def img = docker.image("python:2.7")

            img.pull()
            sshagent(['jenkins-node']) {
              def container = img.inside("-v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro -v ${SSH_AUTH_SOCK}:${SSH_AUTH_SOCK} -e SSH_AUTH_SOCK=${SSH_AUTH_SOCK} -e GIT_SSH_COMMAND='ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'") {
                sh "ls -la"
              }
            }
        }
      }
    }
  }

  post {
    always {
      archive "phpunit_junit.xml"
      junit "phpunit_junit.xml"
    }
  }
}

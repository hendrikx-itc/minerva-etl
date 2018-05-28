pipeline {
  agent none

  stages {
    stage('unit tests') {
      steps {
        script {
          def container = docker.image('python:2.7').inside("-v ${workspace}/src:/src -v ${workspace}/tests:/tests -v ${workspace}/setup.py:/setup.py") {
            sh "python /setup/py install"
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

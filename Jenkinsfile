pipeline {
  agent none

  stages {
    stage('unit tests') {
      steps {
        script {
          def container = docker.image('python:2.7').inside("-v ${WORKSPACE}/src:/src -v ${WORKSPACE}/tests:/tests -v ${WORKSPACE}/setup.py:/setup.py") {
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

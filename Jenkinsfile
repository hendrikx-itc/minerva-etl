pipeline {
  agent any

  stages {
    stage('unit tests') {
      steps {
        script {
          def container = docker.image('python:2.7').inside() {
            sh "python -m pip install ."
          }
        }
      }
    }
  }
}

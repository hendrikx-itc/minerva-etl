pipeline {
    parameters {
        string(
            name: 'MINERVA_VERSION', 
            default: '',
            description: 'Minerva-etl version used in this pipeline'
        )
    }

    agent {
        node {
            label 'git'
        }
    }

    stages {
        stage ('checkout') {
            checkout scm
        }

        stage ('minerva-etl version') {
            steps {
                script {
                    echo ${MINERVA_VERSION}
                }
            }
        }
    }
}
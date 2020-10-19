pipeline {
    parameters {
        string(
            name: 'MINERVA_VERSION', 
            defaultValue: '',
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
            steps {
                checkout scm
            }
        }

        stage ('minerva-etl version') {
            steps {
                script {
                    echo "python3-minerva-etl_${MINERVA_VERSION}_all.deb"
                }
            }
        }
    }
}
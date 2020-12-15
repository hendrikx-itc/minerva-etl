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
                    echo "${MINERVA_VERSION}"
                }
            }
        }

        stage ('run integration tests') {
            agent {
                dockerfile {
                    filename 'Dockerfile.1804.integration_tests'
                    args '--user root --name pytest --network pytest -e TEST_DOCKER_NETWORK=pytest -v ${workspace}/:/source -v /var/run/docker.sock:/var/run/docker.sock -w /source'
                }
            }

            steps {
                script {
                    sh 'lsb_release -a'
                    
                    sh 'printf "deb [trusted=yes] https://packages.hendrikx-itc.nl/hitc/common/bionic/${MINERVA_VERSION}/ bionic main" > /etc/apt/sources.list'
                    sh 'apt-get update && apt-get install python3-minerva-etl'
                                                           
                    sh 'pytest integration_tests/ --suppress-tests-failed-exit-code --junitxml=integrationtest_report.xml'
                    junit 'integrationtest_report.xml'
                }
            }
        }
    }
}

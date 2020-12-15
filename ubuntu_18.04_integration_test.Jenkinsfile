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
                    git branch: 'update-minerva-etl-for-ubuntu-18_04', url: 'ssh://git@gitlab.hendrikx-itc.nl:2022/hitc/Minerva/minerva-etl.git'
                    
                    sh 'ls -la'
                    sh 'pip3 freeze | grep psycopg2'
                    
                    sh 'pytest integration_tests/ --suppress-tests-failed-exit-code --junitxml=integrationtest_report.xml'
                    junit 'integrationtest_report.xml'
                    
                    sh 'lsb_release -a'
                }
            }
        }
    }
}

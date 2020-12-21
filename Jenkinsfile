pipeline {
    parameters {
        string(name: 'PACKAGE_SECTION', defaultValue: 'unstable', description: '')
    }

    agent {
        node {
            label 'git'
        }
    }

    stages {
        stage ('Checout') {
            steps {
                checkout scm
            }
        }

        stage ('Unit Test') {
            agent {
                dockerfile {
                    filename 'Dockerfile.python'
                    args '-v ${workspace}:/source'
                }
            }

            steps {
                script {
                    sh 'pytest ./tests --suppress-tests-failed-exit-code --junitxml=unittest_report.xml'
                    junit 'unittest_report.xml'
                }
            }
        }
        

        stage ('Integration Test') {
            agent {
                dockerfile {
                    filename 'Dockerfile.python'
                    args '--user root --name pytest --network pytest -e TEST_DOCKER_NETWORK=pytest -v ${workspace}:/source -v /var/run/docker.sock:/var/run/docker.sock -w /source'
                }
            }
            
            steps {
                script {
                    sh 'ls -la'
                    sh 'pip3 install .'
                    sh 'pytest ./integration_tests --suppress-tests-failed-exit-code --junitxml=integrationtest_report.xml'
                    junit 'integrationtest_report.xml'
                }
            }
        }       

        stage ('Build') {
            steps {
                // Report status of stage 'build' back to Gitlab
                gitlabCommitStatus(name: 'build') {
                    // Populate changelog file with version information from git tag
                    dir ('debian') {
                        sh './make-changelog'
                    }

                    script {
                        //---------------------------
                        // Build Ubuntu 18.04 package
                        //---------------------------
                        def buildDir1804 = 'pkg-build/1804'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir1804}"

                        sh './package 1804'
                        
                        publishPackages buildDir1804.toString(), "common/bionic/${params.PACKAGE_SECTION}", 'bionic'

                        archiveArtifacts(artifacts: "${buildDir1804}/*")
                    }
                }
            }
        }
    }
}

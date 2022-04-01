pipeline {
    agent {
        node {
            label 'docker'
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
                }
            }

            steps {
                script {
                    sh 'docker-context/run-tests tests --suppress-tests-failed-exit-code --junitxml=unittest_report.xml'
                    junit 'unittest_report.xml'
                }
            }
        }
        

        stage ('Integration Test') {
            agent {
                dockerfile {
                    filename 'Dockerfile.python'
                    args '--user root --network pytest -e TEST_DOCKER_NETWORK=pytest -v ${workspace}:/source -v /var/run/docker.sock:/var/run/docker.sock -w /source'
                }
            }
            
            steps {
                script {
                    sh 'docker-context/run-tests integration_tests --suppress-tests-failed-exit-code --junitxml=integrationtest_report.xml'
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
                        // Build Ubuntu 20.04 package
                        //---------------------------
                        def buildDir2004 = 'pkg-build/2004'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir2004}"

                        sh './package 2004'
                        
                        publishPackages buildDir2004.toString(), "common/bionic/stable", 'bionic'

                        archiveArtifacts(artifacts: "${buildDir2004}/*")
                    }
                }
            }
        }
    }
}

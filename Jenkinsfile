pipeline {
    agent any

    stages {
        stage ('package') {
            steps {
                script {
                    gitlabCommitStatus(name: 'package') {
                        def targetDistribution = 'xenial'
                        def buildDir = 'pkg-build'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir}"
                        sh "mkdir -p ${buildDir}"

                        // Credentials used should be stored in Jenkins under the name 'hitc-docker-registry'
                        docker.withRegistry("https://${docker_registry}", 'hitc-docker-registry') {
                            docker.image('ubuntu-1604-packaging').inside("-v ${workspace}:/package/source -v ${workspace}/${buildDir}:/package/build") {
                                sh 'package'
                            }
                        }

                        sh "scp -r ${buildDir}/. repo-manager@controller.hitc:/var/packages/common/stable"
                        sh "ssh repo-manager@controller.hitc publish-packages common/stable xenial"
                    }
                }
            }
        }
    }
}

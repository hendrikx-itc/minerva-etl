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

        stage ('package') {
            script {
                gitlabCommitStatus(name: 'package') {
                    def imgName = 'ubuntu-1604-packaging'
                    def targetDistribution = 'xenial'
                    def buildDir = 'pkg-build'

                    // Clean the build directory before starting
                    sh "rm -rf ${buildDir}"
                    sh "mkdir -p ${buildDir}"

                    // Credentials used should be stored in Jenkins under the name 'hitc-docker-registry'
                    docker.withRegistry(docker_registry, 'hitc-docker-registry') {
                        def img = docker.image imgName

                        img.pull()

                        def container = img.run("-v ${workspace}:/package/source -v ${workspace}/${buildDir}:/package/build", targetDistribution)

                        sh "docker wait ${container.id}"
                    }

                    sh "scp -r ${buildDir}/. repo-manager@controller.hitc:/var/packages/common/stable"
                    sh "ssh repo-manager@controller.hitc publish-packages common/stable xenial"
                }
            }
        }
    }
}

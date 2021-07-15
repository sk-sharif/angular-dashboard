pipeline {
 agent any
    environment {
        registry = "akanshagiriya/angular"
    registryCredential = 'Docker_cred'
    dockerImage = ''
}
 
  stages {

stage("Deploy to Production"){
            when {
                branch 'master'
            }
            steps { 
                script {
                  echo 'master branch'
                }
              
             }
            post{
                success{
                    echo "Successfully deployed to Production"
                }
                failure{
                    echo "Failed deploying to Production"
                }
            }
        }
stage("Deploy to Staging"){
            when {
                branch 'staging'
            }
            steps {
              echo 'staging'
             }
            post{
                success{
                    echo "Successfully deployed to Staging"
                }
                failure{
                    echo "Failed deploying to Staging"
                }
            }
        }
                stage('Build project A') {
            when {
                changeset "adsbrain-feed-etl/**"
            }
            steps {
                echo 'changed in Build A'
              script {
                sh '''
                cd adsbrain-feed-etl/docker-images/adsbrain-feed/
                    docker build -t ${registry}":$BUILD_NUMBER" .
                    '''
                    docker.withRegistry( '', registryCredential ) {
                      sh 'docker push ${registry}:"$BUILD_NUMBER"'
                    }
                }
            }
        }
        stage('Build project B') {
            when {
                changeset "ch1-2-migration/**"
            }
            steps {
                echo 'changed in Build B'
              script {
                dockerImage = docker.build registry + ":$BUILD_NUMBER"
                    docker.withRegistry( '', registryCredential ) {
                        dockerImage.push("$BUILD_NUMBER")
                        dockerImage.push('latest')
                    }
                }
            }
        }
//    stage('Build Release') {
//               when { tag pattern: "\\d+\\.\\d+\\.\\d", comparator: "REGEXP"}
//      steps {
//        echo "Building ${env.TAG_NAME}"
//      }
//         }
    sh 'git tag --contains | head -1'
//     def tag = sh(returnStdout: true, script: "git tag --contains | head -1").trim()
//     if (tag) {
//         stage("deploy.....") {
//             echo 'deploy'
//         }
//     }
    
//     stage('Deploy') {
//             when {
//     expression {
//         env.TAG_NAME != null
//     }
// }
//             steps {
//                 echo 'Deploying only because this commit is tagged...'
//             }
//         }
    
    }
}

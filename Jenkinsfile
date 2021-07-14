pipeline {
 agent any
    environment {
        
        //put your own environment variables
        registry = "akanshagiriya/angular"
    registryCredential = 'Docker_cred'
    dockerImage = ''
}
 
    stages {
       
//       stage('Building image') {
//       steps{
//         script {
//           dockerImage = docker.build registry + ":$BUILD_NUMBER"
//         }
//       }
//     }
//     stage('Push Image') {
//             steps{
//                 script {
//                     docker.withRegistry( '', registryCredential ) {
//                         dockerImage.push("$BUILD_NUMBER")
//                         dockerImage.push('latest')
//                     }
//                 }
//             }
//             post{
//                 success{
//                     echo "Build and Push Successfully"
//                 }
//                 failure{
//                     echo "Build and Push Failed"
//                 }
//             }
//         }

stage("Deploy to Production"){
            when {
                branch 'master'
            }
            steps { 
                echo 'we are in master'
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
                echo 'we are in staging branch'
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
  stage('Deploy') {
            when { tag "release-*" }
            steps {
                echo 'Deploying only because this commit is tagged...'
            }
        }
    }
}

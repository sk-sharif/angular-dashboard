def customImages

pipeline {
 agent any
  environment {
    registry = "akanshagiriya/angular"
    registryCredential = 'Docker_cred'
  }
 
  stages {
    stage("Checkout Master Branch"){
      when {
        branch 'master'
      }
      steps {
        script {
          echo 'In master branch'
        }
      }
    }
    
    stage("CheckOut Staging Branch"){
      when {
        branch 'staging'
      }
      steps {
        echo 'In staging branch'
      }
    }
    
    stage("CheckOut Test Branch"){
      when {
        branch 'test'
      }
      steps {
        echo 'In test branch'
      }
    }
    
    stage('Build project A') {
      when {
        changeset "adsbrain-feed-etl/**"
        not { branch 'master' }
      }
      steps {
        script {
          docker.withRegistry( '', registryCredential ) {
            def dockerfile = 'Dockerfile'
            def customImage = docker.build("${registry}:${BUILD_NUMBER}", "-f ./adsbrain-feed-etl/docker-images/adsbrain-feed/${dockerfile} ./adsbrain-feed-etl/docker-images/adsbrain-feed/")
            customImage.push()
          }
        }
      }
    }
    
     stage('Build project A master branch') {
      when {
        allOf {
          branch 'master'
          changeset "adsbrain-feed-etl/**"
        }
      }
      steps {
        script {
          docker.withRegistry( '', registryCredential ) {
            def dockerfile = 'Dockerfile'
            customImages = docker.build("${registry}:${BUILD_NUMBER}", "-f ./adsbrain-feed-etl/docker-images/adsbrain-feed/${dockerfile} ./adsbrain-feed-etl/docker-images/adsbrain-feed/")
//             customImage.push()
          }
        }
      }
    }
    
    stage('Build project B') {
      when {
        changeset "ch1-2-migration/**"
        not { branch 'master' }
      }
      steps {
        echo 'Building in Build B'
        script {
          docker.withRegistry( '', registryCredential ) {
            def dockerfile = 'Dockerfile'
            def customImage = docker.build("${registry}:${BUILD_NUMBER}", "-f ./ch1-2-migration/docker-images/ch-entity-validation/${dockerfile} ./ch1-2-migration/docker-images/ch-entity-validation/")
            customImage.push()
          }
        }
      }
    }
    
//     stage('Deploy') {
//       when {
//         branch 'master';
//         expression { sh([returnStdout: true, script: 'echo $TAG_NAME | tr -d \'\n\'']) }     
//       }
//       steps {
//         script {
// //         docker.withRegistry( '', registryCredential ) {
// //             customImages.push()
// //           }
//           echo 'tag'
//         }
//       }
//     }
    
    stage('tag') {
  when {
	  branch 'master'
	  expression { sh([returnStdout: true, script: 'echo $TAG_NAME | tr -d \'\n\'']) } 
  }
  steps {
    echo 'Replace this with your actual deployment steps'
  }
}

    
    
    stage("Deploying Master branch"){
      when {
        branch 'master'
      }
      steps {
        echo 'Deployed Master Branch'
      }
    }
    
    stage("Deploying Staging branch"){
      when {
        branch 'staging'
      }
      steps {
        echo 'Deployed Staging Branch'
      }
    }
    
    stage("Deploying Test branch"){
      when {
        branch 'test'
      }
      steps {
        echo 'Deployed Test Branch'
      }
    }
  }
}

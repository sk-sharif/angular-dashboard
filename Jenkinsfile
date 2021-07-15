pipeline {
 agent any
  environment {
    registry = "akanshagiriya/angular123"
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
      }
      steps {
        echo 'Building in Build A'
        script {
          docker.withRegistry( '', registryCredential ) {
            def dockerfile = 'Dockerfile'
            def customImage = docker.build("${registry}:${BUILD_NUMBER}", "-f ./adsbrain-feed-etl/docker-images/adsbrain-feed/${dockerfile} ./adsbrain-feed-etl/docker-images/adsbrain-feed/")
            customImage.push()
          }
        }
      }
    }
    
    stage('Build project B') {
      when {
        changeset "ch1-2-migration/**"
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
    
//     stage('check') {
//       //         sh 'git tag --contains | head -1'
//       when {
//         tag = sh(returnStdout: true, script: "git tag --contains | head -1").trim()
//       }
//       steps {
//         echo 'tags'
//       }
//     }
    
    
    stage("Deploying Master branch"){
      when { allOf {
          branch 'master';
          changeset "adsbrain-feed-etl/**";
          changeset "ch1-2-migration/**"
      }
      }
      steps {
        echo 'Deployed Master Branch'
      }
    }
    
    stage("Deploying Staging branch"){
      when { allOf {
          branch 'staging';
          changeset "adsbrain-feed-etl/**";
          changeset "ch1-2-migration/**"
      }
      }
      steps {
        echo 'Deployed Staging Branch'
      }
    }
    
    stage("Deploying Test branch"){
      when {
        allOf {
          branch 'test';
          changeset "adsbrain-feed-etl/**";
          changeset "ch1-2-migration/**"
        }
      }
      steps {
        echo 'Deployed Test Branch'
      }
    }
  }
}

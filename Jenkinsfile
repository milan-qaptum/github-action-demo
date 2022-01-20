pipeline {
  environment {
  registry = "nibatandukar/kubernetes-ci-cd"
  registryCredential = 'dockerhub'
  dockerImage = ''
  }    
  agent any 
     stages {
        
        stage('Image Build') {
             steps {
               script {
                   sh '''
                   dockerImage = docker.build registry + ":$BUILD_NUMBER"
                   docker.withRegistry( '', registryCredential ) {
                   dockerImage.push()
                       }
                      '''
                      }
                    }
                }      
            }
}


pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                git 'https://github.com/ISS-infra/qc-tool.git'
                bat "npm install"
            }
        }

        stage('Scan') {
            steps {
                withSonarQubeEnv(installationName: 'sq1') {
                    bat "npm install sonar-scanner"
                    bat 'npx sonar-scanner -X -X -Dsonar.projectKey=mywebapp'
                }
            }
        }
    }
}
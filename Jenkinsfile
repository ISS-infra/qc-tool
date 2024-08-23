pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                // Specify branch if needed, e.g., 'main' or 'master'
                git branch: 'main', url: 'https://github.com/ISS-infra/qc-tool.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                // Ensure npm is installed and the environment is correctly set
                bat 'npm install'
            }
        }

        stage('SonarQube Scan') {
            steps {
                // Ensure SonarQube scanner is installed
                bat 'npm install -g sonar-scanner'
                withSonarQubeEnv('sq1') {
                    // Run SonarQube analysis
                    bat 'sonar-scanner -X -Dsonar.projectKey=mywebapp'
                }
            }
        }
    }

    post {
        always {
            echo 'Pipeline completed.'
        }
        success {
            echo 'Pipeline succeeded.'
        }
        failure {
            echo 'Pipeline failed.'
        }
    }
}

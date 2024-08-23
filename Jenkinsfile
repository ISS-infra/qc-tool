pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                // Specify branch if needed, e.g., 'main' or 'master'
                git branch: 'main', url: 'https://github.com/ISS-infra/qc-tool.git'
            }
        }

        stage('Build') {
            steps {
                // Run npm install
                bat "npm install"
            }
        }

        stage('Scan') {
            steps {
                // SonarQube environment setup
                withSonarQubeEnv('sq1') {
                    // Ensure sonar-scanner is available; if not, install
                    bat 'npm install -g sonar-scanner'
                    // Run the SonarQube analysis
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

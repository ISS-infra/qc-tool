pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                // Checkout the source code from the specified branch
                git branch: 'main', url: 'https://github.com/ISS-infra/qc-tool.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                // Install npm dependencies
                script {
                    try {
                        bat "npm install"
                    } catch (Exception e) {
                        error "Failed to install npm dependencies."
                    }
                }
            }
        }

        stage('SonarQube Scan') {
            steps {
                // Setup SonarQube environment and run the scanner
                script {
                    withSonarQubeEnv('sq1') {
                        try {
                            // Install sonar-scanner if not available
                            bat 'npm install -g sonar-scanner'
                            // Run SonarQube analysis
                            bat 'sonar-scanner -X -Dsonar.projectKey=mywebapp'
                        } catch (Exception e) {
                            error "SonarQube scan failed."
                        }
                    }
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
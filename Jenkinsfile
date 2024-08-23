pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                // Clean up the workspace to avoid potential issues with a corrupted or incomplete workspace
                deleteDir()

                // Specify the branch and URL for the Git repository
                git branch: 'main', url: 'https://github.com/ISS-infra/qc-tool.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                script {
                    try {
                        // Check Node.js and npm versions to ensure they are installed
                        bat "node --version"
                        bat "npm --version"

                        // Install npm dependencies with increased verbosity for troubleshooting
                        bat "npm install --verbose"
                    } catch (Exception e) {
                        // Handle the error if npm install fails
                        error "Failed to install npm dependencies."
                    }
                }
            }
        }

        stage('SonarQube Scan') {
            steps {
                // SonarQube environment setup
                withSonarQubeEnv('sq1') {
                    // Ensure sonar-scanner is installed
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

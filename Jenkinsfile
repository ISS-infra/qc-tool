pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/ISS-infra/qc-tool.git'
            }
        }

        stage('Install Dependencies') {
            steps {
                script {
                    if (isUnix()) {
                        // Unix-based systems (Linux, macOS)
                        sh 'npm install'
                    } else {
                        // Windows systems
                        bat 'npm install'
                    }
                }
            }
        }

        stage('SonarQube Scan') {
            steps {
                script {
                    if (isUnix()) {
                        // Unix-based systems (Linux, macOS)
                        sh 'npm install -g sonar-scanner'
                        withSonarQubeEnv('sq1') {
                            sh 'sonar-scanner -X -Dsonar.projectKey=mywebapp'
                        }
                    } else {
                        // Windows systems
                        bat 'npm install -g sonar-scanner'
                        withSonarQubeEnv('sq1') {
                            bat 'sonar-scanner -X -Dsonar.projectKey=mywebapp'
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

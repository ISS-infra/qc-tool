pipeline {
    agent any

    tools {
        nodejs 'NodeJS' // Use the name configured in the NodeJS plugin
    }

    stages {
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

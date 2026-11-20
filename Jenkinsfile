pipeline {
  agent { label 'cci-jdk21-ub24-sm' }

  environment {
    CANAVERAL_ARTIFACTORY_READ_USER = credentials('CANAVERAL_ARTIFACTORY_READ_USER')
    CANAVERAL_ARTIFACTORY_READ_PASSWORD = credentials('CANAVERAL_ARTIFACTORY_READ_PASSWORD')
    TOKEN_VENDING_MACHINE_HASH = credentials('TOKEN_VENDING_MACHINE_HASH')
    USE_ARTIFACTORY = credentials('USE_ARTIFACTORY')

    CANAVERAL_BUILD_SYSTEM = "maven"
    CANAVERAL_PACKAGE_TOOLS = "noop"
    CANAVERAL_MAVEN_VERSION = "3.9.9"
    CANAVERAL_BUILD_MAVEN_TARGETS = "clean install --no-transfer-progress"

    CANAVERAL_USE_LEGACY_PIPELINE_MODE = 0
    CANAVERAL_USE_BLUEPRINT_PROXY = 1
    CAN_JENKINS = "true"
  }

  stages {
    stage('Install Build Tools') {
      steps {
        script {
          installBuildTools()
        }
      }
    }

    stage('Setup Environment') {
      steps {
        script {
          pyenvSh(
            'Run setup environment',
            'python -m canaveral_build_tools.core.setup_environment'
          )

          def descFile = "${env.WORKSPACE}/.workspace/.canaveral_build_description"
          if (fileExists(descFile)) {
            currentBuild.description = readFile(descFile).trim()
          }
        }
      }
    }

    stage('Post Checkout') {
      steps {
        script {
          pyenvSh(
            'Post Checkout',
            'python -m canaveral_build_tools.core.post_checkout'
          )
        }
      }
    }

    stage('Dependency Setup') {
      steps {
        script {
          pyenvSh(
            'Run dependency setup',
            'python -m canaveral_build_tools.core.dependency_setup'
          )
        }
      }
    }

    stage('Build') {
      steps {
        script {
          pyenvSh(
            'Run build',
            'python -m canaveral_build_tools.core.build'
          )
        }
      }
    }
  }
}

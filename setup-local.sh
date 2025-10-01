#!/bin/bash

echo "🚀 Setting up Message Transfer Application for local development"
echo ""

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "📋 Creating .env file from template..."
    cp env.example .env
    echo "✅ Created .env file"
    echo "⚠️  IMPORTANT: Edit .env file with your actual Azure Service Bus and Redis credentials"
    echo ""
else
    echo "✅ .env file already exists"
fi

# Check if Redis is running locally
echo "🔍 Checking Redis..."
if command -v redis-cli &> /dev/null; then
    if redis-cli ping &> /dev/null; then
        echo "✅ Redis is running locally"
    else
        echo "⚠️  Redis is installed but not running. Start it with:"
        echo "   brew services start redis  # macOS with Homebrew"
        echo "   redis-server              # Manual start"
    fi
else
    echo "⚠️  Redis not found. Install it with:"
    echo "   brew install redis         # macOS with Homebrew"
    echo "   # OR use Azure Cache for Redis (configure in .env)"
fi

echo ""
echo "📝 Next steps:"
echo "1. Edit .env file with your credentials:"
echo "   - Azure Service Bus connection strings"
echo "   - Redis configuration"
echo ""
echo "2. Run the application:"
echo "   ./gradlew bootRun"
echo ""
echo "3. Test the health endpoint:"
echo "   curl http://localhost:8080/api/health"
echo ""
echo "📚 For detailed setup instructions, see README.md"
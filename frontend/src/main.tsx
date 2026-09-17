import React, { Component, type ErrorInfo, type ReactNode } from 'react';
import ReactDOM from 'react-dom/client';
import 'leaflet/dist/leaflet.css';
import './styles.css';
import App from './App';

class AppErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  componentDidCatch(error: Error, info: ErrorInfo) { console.error('PRALAY X authority render error:', error, info); }
  render() {
    if (this.state.error) return <div className="loading-screen"><h2>PRALAY X could not load this view</h2><p>Please refresh the page. If the issue continues, restart the frontend and backend.</p><pre style={{maxWidth:'90%',whiteSpace:'pre-wrap',color:'#ffb8b8'}}>{this.state.error.message}</pre></div>;
    return this.props.children;
  }
}

ReactDOM.createRoot(document.getElementById('root')!).render(<React.StrictMode><AppErrorBoundary><App /></AppErrorBoundary></React.StrictMode>);

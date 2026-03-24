export default function MainLayout({ children }) {
  return (
    <div className="flex min-h-screen bg-bg">

      {/* Sidebar */}
      <aside className="w-[240px] bg-white border-r border-border px-6 py-8">
        <h2 className="text-lg font-semibold text-textMain mb-8">
          ShariahAI
        </h2>

        <nav className="space-y-3 text-sm">
          <p className="text-primary font-medium">Dashboard</p>
          <p className="text-textSub">Data Analysis</p>
          <p className="text-textSub">Scholar Review</p>
          <p className="text-textSub">Audit Logs</p>
        </nav>
      </aside>

      {/* Content */}
      <main className="flex-1 px-10 py-8">

        {/* Header */}
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-xl font-semibold text-textMain">
            Dashboard
          </h1>
          <span className="text-sm text-textSub">Admin</span>
        </div>

        {children}
      </main>
    </div>
  );
}
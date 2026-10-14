//! Tetris — the Dos Navigator tribute. The board, the seven pieces, gravity
//! on a wall clock, scoring; the widget is `ui::tetris`.

use std::time::{Duration, Instant};

pub const WIDTH: usize = 10;
pub const HEIGHT: usize = 20;

/// A piece kind (its colour and shape index).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    I,
    O,
    T,
    S,
    Z,
    J,
    L,
}

impl Kind {
    pub const ALL: [Kind; 7] = [Kind::I, Kind::O, Kind::T, Kind::S, Kind::Z, Kind::J, Kind::L];

    /// The cells of the spawn orientation, around the piece's origin.
    fn cells(self) -> [(i32, i32); 4] {
        match self {
            Kind::I => [(-1, 0), (0, 0), (1, 0), (2, 0)],
            Kind::O => [(0, 0), (1, 0), (0, 1), (1, 1)],
            Kind::T => [(-1, 0), (0, 0), (1, 0), (0, 1)],
            Kind::S => [(0, 0), (1, 0), (-1, 1), (0, 1)],
            Kind::Z => [(-1, 0), (0, 0), (0, 1), (1, 1)],
            Kind::J => [(-1, 0), (0, 0), (1, 0), (1, 1)],
            Kind::L => [(-1, 0), (0, 0), (1, 0), (-1, 1)],
        }
    }
}

/// The falling piece: kind, its cells (rotated), and its origin on the board.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Piece {
    pub kind: Kind,
    pub cells: [(i32, i32); 4],
    pub x: i32,
    pub y: i32,
}

impl Piece {
    fn spawn(kind: Kind) -> Self {
        Self {
            kind,
            cells: kind.cells(),
            x: WIDTH as i32 / 2 - 1,
            y: 0,
        }
    }

    /// The board coordinates of the four cells.
    pub fn board_cells(&self) -> [(i32, i32); 4] {
        self.cells.map(|(cx, cy)| (self.x + cx, self.y + cy))
    }

    /// Rotated clockwise around the origin (the O piece is symmetric).
    fn rotated(&self) -> Self {
        if self.kind == Kind::O {
            return self.clone();
        }
        Self {
            cells: self.cells.map(|(cx, cy)| (-cy, cx)),
            ..self.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Running,
    Paused,
    GameOver,
}

pub struct Tetris {
    /// Settled cells, row 0 at the top.
    pub board: [[Option<Kind>; WIDTH]; HEIGHT],
    pub current: Piece,
    pub next: Kind,
    pub score: u32,
    pub lines: u32,
    pub phase: Phase,
    last_drop: Instant,
    /// Set by a soft drop (↓) so the next gravity tick does not double it.
    bag: Vec<Kind>,
}

impl Tetris {
    pub fn new() -> Self {
        let mut bag = Vec::new();
        let first = Self::draw(&mut bag);
        let next = Self::draw(&mut bag);
        Self {
            board: [[None; WIDTH]; HEIGHT],
            current: Piece::spawn(first),
            next,
            score: 0,
            lines: 0,
            phase: Phase::Running,
            last_drop: Instant::now(),
            bag,
        }
    }

    /// The seven-bag: every kind once per seven pieces, shuffled.
    fn draw(bag: &mut Vec<Kind>) -> Kind {
        if bag.is_empty() {
            use rand::seq::SliceRandom;
            let mut kinds = Kind::ALL.to_vec();
            kinds.shuffle(&mut rand::rng());
            *bag = kinds;
        }
        bag.pop().expect("refilled")
    }

    /// Lines ÷ 10, from 1.
    pub fn level(&self) -> u32 {
        self.lines / 10 + 1
    }

    /// Gravity interval: faster per level, never under 100 ms.
    fn drop_interval(&self) -> Duration {
        Duration::from_millis(800u64.saturating_sub(70 * (self.level() as u64 - 1)).max(100))
    }

    fn fits(&self, piece: &Piece) -> bool {
        piece.board_cells().iter().all(|&(x, y)| {
            (0..WIDTH as i32).contains(&x)
                && (0..HEIGHT as i32).contains(&y)
                && self.board[y as usize][x as usize].is_none()
        })
    }

    /// Try to place `piece`; `true` when it fits and became the current one.
    fn try_move(&mut self, piece: Piece) -> bool {
        if self.fits(&piece) {
            self.current = piece;
            true
        } else {
            false
        }
    }

    pub fn move_left(&mut self) {
        if self.phase == Phase::Running {
            let p = Piece {
                x: self.current.x - 1,
                ..self.current.clone()
            };
            self.try_move(p);
        }
    }

    pub fn move_right(&mut self) {
        if self.phase == Phase::Running {
            let p = Piece {
                x: self.current.x + 1,
                ..self.current.clone()
            };
            self.try_move(p);
        }
    }

    /// Rotate clockwise, kicking off a wall by up to two cells.
    pub fn rotate(&mut self) {
        if self.phase != Phase::Running {
            return;
        }
        let rotated = self.current.rotated();
        for dx in [0, -1, 1, -2, 2] {
            let p = Piece {
                x: rotated.x + dx,
                ..rotated.clone()
            };
            if self.try_move(p) {
                return;
            }
        }
    }

    /// One row down; `false` when the piece landed (and was locked).
    fn step_down(&mut self) -> bool {
        let p = Piece {
            y: self.current.y + 1,
            ..self.current.clone()
        };
        if self.try_move(p) {
            true
        } else {
            self.lock();
            false
        }
    }

    /// ↓: one row now, a point for the effort.
    pub fn soft_drop(&mut self) {
        if self.phase == Phase::Running && self.step_down() {
            self.score += 1;
            self.last_drop = Instant::now();
        }
    }

    /// Space: straight to the floor, two points a row.
    pub fn hard_drop(&mut self) {
        if self.phase != Phase::Running {
            return;
        }
        while self.step_down() {
            self.score += 2;
        }
        self.last_drop = Instant::now();
    }

    pub fn toggle_pause(&mut self) {
        self.phase = match self.phase {
            Phase::Running => Phase::Paused,
            Phase::Paused => {
                self.last_drop = Instant::now();
                Phase::Running
            }
            Phase::GameOver => Phase::GameOver,
        };
    }

    /// Gravity: called every frame, drops when the interval has passed.
    pub fn tick(&mut self) {
        if self.phase != Phase::Running || self.last_drop.elapsed() < self.drop_interval() {
            return;
        }
        self.step_down();
        self.last_drop = Instant::now();
    }

    /// Settle the current piece, clear full rows, spawn the next; a spawn
    /// that does not fit is game over.
    fn lock(&mut self) {
        for (x, y) in self.current.board_cells() {
            if (0..HEIGHT as i32).contains(&y) && (0..WIDTH as i32).contains(&x) {
                self.board[y as usize][x as usize] = Some(self.current.kind);
            }
        }
        let cleared = self.clear_lines();
        if cleared > 0 {
            self.lines += cleared;
            self.score += [0, 40, 100, 300, 1200][cleared as usize] * self.level();
        }
        let kind = std::mem::replace(&mut self.next, Self::draw(&mut self.bag));
        let spawned = Piece::spawn(kind);
        if self.fits(&spawned) {
            self.current = spawned;
        } else {
            self.current = spawned;
            self.phase = Phase::GameOver;
        }
    }

    fn clear_lines(&mut self) -> u32 {
        let mut kept: Vec<[Option<Kind>; WIDTH]> = self
            .board
            .iter()
            .copied()
            .filter(|row| row.iter().any(|c| c.is_none()))
            .collect();
        let cleared = (HEIGHT - kept.len()) as u32;
        let mut board = [[None; WIDTH]; HEIGHT];
        let offset = HEIGHT - kept.len();
        for (i, row) in kept.drain(..).enumerate() {
            board[offset + i] = row;
        }
        self.board = board;
        cleared
    }
}

impl Default for Tetris {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn game_with(kind: Kind) -> Tetris {
        let mut g = Tetris::new();
        g.current = Piece::spawn(kind);
        g
    }

    #[test]
    fn hard_drop_lands_on_the_floor_and_spawns_the_next() {
        let mut g = game_with(Kind::O);
        g.hard_drop();
        assert_eq!(g.board[HEIGHT - 1][4], Some(Kind::O));
        assert_eq!(g.board[HEIGHT - 2][5], Some(Kind::O));
        assert_eq!(g.current.y, 0, "a fresh piece spawned at the top");
        assert_eq!(g.phase, Phase::Running);
    }

    #[test]
    fn full_rows_clear_and_score() {
        let mut g = game_with(Kind::I);
        // Fill the bottom row but a 4-wide gap at x 3..7 for a horizontal I.
        for x in (0..3).chain(7..WIDTH) {
            g.board[HEIGHT - 1][x] = Some(Kind::O);
        }
        g.current.x = 4; // I cells span x-1..x+2 = 3..6
        g.hard_drop();
        assert!(g.board[HEIGHT - 1].iter().all(|c| c.is_none()), "the row is gone");
        assert_eq!(g.lines, 1);
        assert_eq!(g.score, 40 + 2 * (HEIGHT as u32 - 1));
    }

    #[test]
    fn rotation_kicks_off_the_wall() {
        let mut g = game_with(Kind::I);
        g.current.x = 1;
        g.current.y = 1;
        g.rotate(); // vertical I at x=1 (rows 0..4) needs no kick
        assert!(g.current.board_cells().iter().all(|&(x, _)| x == 1));
        g.current.x = 0; // against the wall
        g.rotate(); // back to horizontal: x-1..x+2 would start at -1, kicked right
        let xs: Vec<i32> = g.current.board_cells().iter().map(|&(x, _)| x).collect();
        assert!(xs.iter().all(|x| (0..WIDTH as i32).contains(x)), "{xs:?}");
        assert_eq!(xs.iter().min(), Some(&0));
    }

    #[test]
    fn a_blocked_spawn_ends_the_game() {
        let mut g = game_with(Kind::O);
        // A stack to row 2 with a hole in every row (so nothing clears), and
        // a cell under the spawn: the piece locks at the top and the next one
        // has nowhere to go.
        for row in g.board.iter_mut().skip(2) {
            for c in row.iter_mut().skip(1) {
                *c = Some(Kind::O);
            }
        }
        g.board[1][4] = Some(Kind::O);
        g.hard_drop();
        assert_eq!(g.phase, Phase::GameOver);
    }
}

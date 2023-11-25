use rayon::prelude::*;
use rand::distributions::{Distribution, Uniform};

const VALUES:usize = 1 << 30;

fn main() {
    let mut data = vec![0_i32; VALUES];
    let mut rng = rand::thread_rng();
    let die = Uniform::new(0,  i32::MAX) ;

    // initialize data
    data.iter_mut()
        .for_each(|value|{
            *value = die.sample(&mut rng);
        });

    // run an time algorithm
    let before = std::time::Instant::now();
    bucket_sort(&mut data);
    // data.par_sort_unstable();
    let now = std::time::Instant::now();

    println!("Time: {} s", (now - before).as_secs_f32());
    println!("Time: {} ms", (now - before).as_millis());
    
    // check if sorted
    let is_sorted = data.par_windows(2)
        .map(|values| values[0] <= values[1])
        .reduce(|| true, |a, b| a && b);

    println!("Is sorted: {}", is_sorted);
}

fn bucket_sort(vector:&mut Vec<i32>) {
    // Gather statics of the values, for better splitting the buckets
    let min = vector.par_iter().min().unwrap_or(&i32::MIN);
    let max = vector.par_iter().max().unwrap_or(&i32::MAX);

    // Calculate the number of buckets and the step size
    let num_buckets = (vector.len() as f32).log2().ceil() as usize;
    let range_of_values = max - min;
    let step = ((range_of_values + 1) as f32 / num_buckets as f32).ceil() as usize;

    /*
    The code bellow is very compact and represent a lot of stuff happening.
    First a parallel iterator of indexes from 0 to the number of buckets is created.
    Then the flat_map is used, it will apply the map operation for each index.
    The operation is a filter of the values that should be in the bucket of that index.
    The original vector filtered is then sorted and returned.
    The flat_map will then concatenate all the vectors returned by the map operation.
    And to finish the vector is replaced by the new vector sorted.
     */
    *vector = (0..num_buckets).into_par_iter().flat_map(|index| {
        let mut bucket = vector.par_iter()
            .cloned()
            .filter(|value| {
                let bucket_index_to_place = (value - min) as usize / step;
                bucket_index_to_place == index
            })
            .collect::<Vec<i32>>();
        bucket.par_sort_unstable();
        bucket
    }).collect::<Vec<_>>();
}
